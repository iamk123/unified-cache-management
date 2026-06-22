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
import multiprocessing
import os
import secrets

import torch
import torch_npu

from ucm.store.pipeline.connector import UcmPipelineStore

WORKER_NUMBER = int(os.environ.get("LOCAL_RANK_SIZE", "2"))
TENSOR_SIZE = 4096
TENSOR_NUMBER = 4
BLOCK_NUMBER = 8


def make_tensors(device):
    return [
        [
            torch.rand([TENSOR_SIZE // 2], dtype=torch.bfloat16, device=device)
            for _ in range(TENSOR_NUMBER)
        ]
        for _ in range(BLOCK_NUMBER)
    ]


def create_posix_store(storage_dir, device_id):
    block_size = TENSOR_SIZE * TENSOR_NUMBER
    return UcmPipelineStore(
        {
            "store_pipeline": "Posix",
            "device_id": device_id,
            "tensor_size": TENSOR_SIZE,
            "shard_size": block_size,
            "block_size": block_size,
            "storage_backends": [storage_dir],
            "io_direct": False,
        }
    )


def create_yuanrong_store(storage_dir, unique_id, device_id):
    block_size = TENSOR_SIZE * TENSOR_NUMBER
    return UcmPipelineStore(
        {
            "store_pipeline": "Yuanrong|Posix",
            "device_id": device_id,
            "tensor_size_list": [TENSOR_SIZE] * TENSOR_NUMBER,
            "shard_size": block_size,
            "block_size": block_size,
            "storage_backends": [storage_dir],
            "io_direct": False,
            "yuanrong_host": os.environ.get("YUANRONG_HOST", "127.0.0.1"),
            "yuanrong_port": int(os.environ.get("YUANRONG_PORT", "9088")),
            "yuanrong_enable_remote_h2d": True,
            "local_rank_size": WORKER_NUMBER,
            "unique_id": unique_id,
            "share_buffer_capacity_gb": 1,
            "timeout_ms": 10000,
        }
    )


def load_worker(storage_dir, unique_id, device_id, barrier, block_ids, expected):
    torch.npu.set_device(device_id)
    store = create_yuanrong_store(storage_dir, unique_id, device_id)
    output = make_tensors(f"npu:{device_id}")
    barrier.wait()
    task = store.load(block_ids, [0] * BLOCK_NUMBER, output)
    store.wait(task)
    for expected_row, output_row in zip(expected, output):
        for expected_tensor, output_tensor in zip(expected_row, output_row):
            assert torch.equal(expected_tensor, output_tensor.cpu())


def main():
    multiprocessing.set_start_method("spawn", force=True)
    storage_dir = os.environ.get("STORAGE_DIR", "/mnt/test")
    os.makedirs(storage_dir, exist_ok=True)

    block_ids = [secrets.token_bytes(16) for _ in range(BLOCK_NUMBER)]
    source = make_tensors("cpu")
    expected = [[tensor.clone() for tensor in row] for row in source]

    posix = create_posix_store(storage_dir, 0)
    task = posix.dump(block_ids, [0] * BLOCK_NUMBER, source)
    posix.wait(task)

    unique_id = secrets.token_hex(8)
    barrier = multiprocessing.Barrier(WORKER_NUMBER)
    workers = [
        multiprocessing.Process(
            target=load_worker,
            args=(storage_dir, unique_id, rank, barrier, block_ids, expected),
        )
        for rank in range(WORKER_NUMBER)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
        assert worker.exitcode == 0


if __name__ == "__main__":
    os.environ.setdefault("UC_LOGGER_LEVEL", "info")
    main()
