import asyncio
import inspect
import os
import threading
from collections import OrderedDict
from typing import List, Optional, Tuple, Union, no_type_check
from lmcache.experimental.memory_management import MemoryFormat
from concurrent.futures import ThreadPoolExecutor

import dingodb

import yaml

from dataclasses import dataclass
import torch


from lmcache.experimental.memory_management import MemoryAllocatorInterface, MemoryObj

from lmcache.experimental.storage_backend.connector.base_connector import (
    RemoteConnector,
)
from lmcache.experimental.storage_backend.evictor import LRUEvictor, PutStatus
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey


logger = init_logger(__name__)


@dataclass
class DingoStoreCacheMetadata:
    size: int
    shape: torch.Size
    dtype: Optional[torch.dtype]
    fmt: MemoryFormat


@dataclass
class DingoStoreConfig:
    max_dingostore_size: int
    prefix_key: str

    @staticmethod
    def load_config(file_path: str) -> "DingoStoreConfig":
        """
        Load the DingoStore configuration from a YAML file
        """
        with open(file_path, "r") as f:
            config = yaml.safe_load(f)

        if "max_dingostore_size" not in config:
            raise ValueError("Missing required field: max_dingostore_size")
        if "prefix_key" not in config:
            raise ValueError("Missing required field: prefix_key")

        return DingoStoreConfig(
            max_dingostore_size=config.get("max_dingostore_size"),
            prefix_key=config.get("prefix_key"),
        )

    @staticmethod
    def from_env() -> "DingoStoreConfig":
        """
        Load the DingoStore configuration from the environment variable
        """
        config_file_path = os.getenv("DINGOSTORE_CONFIG_FILE")
        if config_file_path is None:
            raise ValueError("DINGOSTORE_CONFIG_FILE environment variable not set")
        return DingoStoreConfig.load_config(config_file_path)


class DingoStoreConnector(RemoteConnector):
    """
    The remote url should start with "dingostore://" and only have three host-port pair
    """

    def __init__(
        self,
        hosts_and_ports: List[Tuple[str, Union[str, int]]],
        loop: asyncio.AbstractEventLoop,
        memory_allocator: MemoryAllocatorInterface,
    ):

        logger.info(f"Connecting to DingoStore at {hosts_and_ports}")

        host_str = ",".join(f"{host}:{port}" for host, port in hosts_and_ports)

        try:
            from dingodb import SDKRawKVDingoDB, SDKClient
        except ImportError as e:
            raise ImportError(
                "DingoStore SDK not found. Please install the DingoStore SDK to use this connector."
            ) from e

        try:
            self.config = DingoStoreConfig.from_env()
            self.evictor = LRUEvictor(self.config.max_dingostore_size)

        except Exception as e:
            logger.error(f"Failed to load DingoStore configuration: {e}")
            raise

        try:
            self.sdk_client = SDKClient(host_str)
            self.connection = SDKRawKVDingoDB(self.sdk_client)
        except Exception as e:
            logger.error(f"Failed to connect to DingoStore: %s", e)
            raise

        self.memory_allocator = memory_allocator
        self.dict: OrderedDict[CacheEngineKey, DingoStoreCacheMetadata] = OrderedDict()
        self.dict_lock = threading.Lock()
        self.loop = loop

    async def exists(self, key: CacheEngineKey) -> bool:
        with self.dict_lock:
            logger.info(f"{key} exists in DingoStore: {key in self.dict}")
            return key in self.dict

    async def get(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        self.dict_lock.acquire()
        if key not in self.dict:
            self.dict_lock.release()
            return None

        self.evictor.update_on_hit(key, self.dict)

        size = self.dict[key].size
        shape = self.dict[key].shape
        dtype = self.dict[key].dtype
        fmt = self.dict[key].fmt

        self.dict_lock.release()

        memory_obj = self.memory_allocator.allocate(
            shape,
            dtype,
            fmt,
        )

        if memory_obj is None:
            logger.warning("Failed to allocate memory during remote receive")
            return None

        try:
            kv_str = self.connection.rawkv_get(self.config.prefix_key + key.to_string())

            logger.info(f"Got {key.to_string()} from DingoStore")
        except Exception as e:
            logger.error(f"Failed to get {key} from DingoStore: %s", e)
            return None

        assert not inspect.isawaitable(kv_str)

        if kv_str is None:
            logger.warning("Failed to get data from DingoStore")
            self.dict_lock.acquire()
            del self.dict[key]
            self.dict_lock.release()
            return None

        byte_array = kv_str.encode("latin1")

        view = memoryview(memory_obj.byte_array).cast("B")
        view[:size] = byte_array

        return memory_obj

    async def put(self, key: CacheEngineKey, memory_obj: MemoryObj):

        evict_keys, put_status = self.evictor.update_on_put(
            self.dict, len(memory_obj.byte_array)
        )
        if put_status == PutStatus.ILLEGAL:
            return

        for evict_key in evict_keys:
            self.remove(evict_key)

        byte_array = memory_obj.byte_array

        try:
            # rawkv_put requires str, so we need to convert the bytearray to str
            if isinstance(byte_array, memoryview):
                kv_bytes = byte_array.tobytes()
            else:
                kv_bytes = byte_array  # already bytes

            kv_str = kv_bytes.decode("latin1")
            await self.loop.run_in_executor(
                None,
                self.connection.rawkv_put_if_absent,
                self.config.prefix_key + key.to_string(),
                kv_str,
            )

            logger.info(f"Put {key.to_string()} to DingoStore")

        except Exception as e:
            logger.error(f"Failed to put {key} to DingoStore: %s", e)
            return

        shape = memory_obj.get_shape()
        dtype = memory_obj.get_dtype()
        memory_format = memory_obj.get_memory_format()

        with self.dict_lock:
            # Need to do reinsert to update cache recency
            if key in self.dict:
                self.dict.pop(key)

            self.dict[key] = DingoStoreCacheMetadata(
                len(byte_array), shape, dtype, memory_format
            )

        self.memory_allocator.ref_count_down(memory_obj)

    def remove(self, key: CacheEngineKey) -> None:
        self.dict_lock.acquire()
        self.dict.pop(key)
        self.dict_lock.release()

        try:
            self.connection.rawkv_delete(self.config.prefix_key + key.to_string())
            logger.info(f"Removed {key} from DingoStore")
        except Exception as e:
            logger.error(f"Failed to remove {key} from DingoStore: %s", e)

    async def list(self) -> List[CacheEngineKey]:
        with self.dict_lock:
            return list(self.dict.keys())

    @no_type_check
    async def close(self) -> None:
        logger.info("Closed the dingostore connection")
