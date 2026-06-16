# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
from dataclasses import dataclass
from typing import Dict, List, Sequence

import torch

from ucm.store.ucmstore_v1 import Task, UcmKVStoreBaseV1


DEFAULT_TIMEOUT_MS = 60000
DEFAULT_KEY_PREFIX = "ucm"


@dataclass
class UcmYuanrongTask(Task):
    future: object
    keys: List[str]


class UcmYuanrongStore(UcmKVStoreBaseV1):
    def __init__(self, config: Dict[str, object]) -> None:
        super().__init__(config)
        self.host = self._require(config, "host")
        self.port = self._require(config, "port")
        self.device_id = int(config.get("device_id"))
        self.timeout_ms = int(config.get("timeout_ms", DEFAULT_TIMEOUT_MS))
        self.key_prefix = str(config.get("key_prefix", DEFAULT_KEY_PREFIX))
        self.tensor_size_list = self._normalize_tensor_size_list(config)

        try:
            from yr.datasystem.hetero_client import Blob, DeviceBlobList, HeteroClient
            from yr.datasystem.kv_client import SetParam
        except ImportError as exc:
            raise ImportError(
                "Please install yuanrong datasystem or add it to PYTHONPATH "
                "before using UcmYuanrongStore."
            ) from exc

        self.Blob = Blob
        self.DeviceBlobList = DeviceBlobList
        self.SetParam = SetParam
        self.client = HeteroClient(self.host, int(self.port))
        self.client.init()

    @staticmethod
    def _require(config: Dict[str, object], key: str) -> object:
        value = config.get(key)
        if value is None:
            raise ValueError(f"Missing required yuanrong store config: {key}.")
        return value

    @staticmethod
    def _normalize_tensor_size_list(config: Dict[str, object]) -> List[int] | None:
        if "tensor_size_list" in config:
            value = config["tensor_size_list"]
            if not isinstance(value, list) or not value:
                raise ValueError("tensor_size_list must be a non-empty list.")
            return [int(size) for size in value]
        if "shard_size" in config:
            return [int(config["shard_size"])]
        if "tensor_size" in config:
            return [int(config["tensor_size"])]
        return None

    def _encode_key(self, block_id: bytes, shard_index: int) -> str:
        return f"{self.key_prefix}:{block_id.hex()}:{shard_index}"

    def _build_keys(
        self, block_ids: Sequence[bytes], shard_index: Sequence[int] | None
    ) -> List[str]:
        if shard_index is None or len(shard_index) == 0:
            shard_index = [0] * len(block_ids)
        if len(block_ids) != len(shard_index):
            raise ValueError("block_ids and shard_index must have the same length.")
        return [
            self._encode_key(block_id, int(index))
            for block_id, index in zip(block_ids, shard_index)
        ]

    def _blob_size(self, blob_index: int) -> int:
        if self.tensor_size_list is None:
            raise ValueError(
                "Missing transfer size config: tensor_size_list, shard_size, "
                "or tensor_size."
            )
        if len(self.tensor_size_list) == 1:
            return self.tensor_size_list[0]
        if blob_index >= len(self.tensor_size_list):
            raise ValueError("tensor_size_list is shorter than address row width.")
        return self.tensor_size_list[blob_index]

    def _build_blob_lists(
        self, block_ids: Sequence[bytes], addr_rows: Sequence[Sequence[int]]
    ) -> List[object]:
        if len(block_ids) != len(addr_rows):
            raise ValueError("block_ids and address rows must have the same length.")
        blob_lists = []
        for row in addr_rows:
            blobs = [
                self.Blob(int(ptr), self._blob_size(blob_index))
                for blob_index, ptr in enumerate(row)
            ]
            if not blobs:
                raise ValueError("address rows must not contain empty rows.")
            blob_lists.append(self.DeviceBlobList(self.device_id, blobs))
        return blob_lists

    def cc_store(self) -> int:
        return 0

    def lookup(self, block_ids: List[bytes]) -> List[bool]:
        keys = self._build_keys(block_ids, [])
        return self.client.exist(keys)

    def lookup_on_prefix(self, block_ids: List[bytes]) -> int:
        for index, hit in enumerate(self.lookup(block_ids)):
            if not hit:
                return index - 1
        return len(block_ids) - 1

    def prefetch(self, block_ids: List[bytes]) -> None:
        pass

    def load(
        self,
        block_ids: List[bytes],
        shard_index: List[int],
        dst_tensor: List[List[torch.Tensor]],
    ) -> Task:
        return self.load_data(
            block_ids,
            shard_index,
            [[tensor.data_ptr() for tensor in row] for row in dst_tensor],
        )

    def dump(
        self,
        block_ids: List[bytes],
        shard_index: List[int],
        src_tensor: List[List[torch.Tensor]],
    ) -> Task:
        return self.dump_data(
            block_ids,
            shard_index,
            [[tensor.data_ptr() for tensor in row] for row in src_tensor],
        )

    def load_data(
        self,
        block_ids: List[bytes],
        shard_index: List[int],
        dst_addr,
    ) -> Task:
        keys = self._build_keys(block_ids, shard_index)
        blob_lists = self._build_blob_lists(block_ids, self._to_rows(dst_addr))
        future = self.client.async_mget_h2d(keys, blob_lists, self.timeout_ms)
        return UcmYuanrongTask(future=future, keys=keys)

    def dump_data(
        self,
        block_ids: List[bytes],
        shard_index: List[int],
        src_addr,
        prerequisite_handle: int = 0,
    ) -> Task:
        keys = self._build_keys(block_ids, shard_index)
        blob_lists = self._build_blob_lists(block_ids, self._to_rows(src_addr))
        future = self.client.async_mset_d2h(keys, blob_lists, self.SetParam())
        return UcmYuanrongTask(future=future, keys=keys)

    @staticmethod
    def _to_rows(addr) -> List[List[int]]:
        if hasattr(addr, "tolist"):
            addr = addr.tolist()
        return [list(row) for row in addr]

    def wait(self, task: Task) -> None:
        failed_keys = task.future.get(self.timeout_ms)
        if failed_keys:
            raise RuntimeError(
                f"Transfer failed for {len(failed_keys)} keys: {failed_keys[:3]}"
            )

    def check(self, task: Task) -> bool:
        return False
