# YuanRongStore 方案设计

问题：存在io_direct=true 内存对齐问题

## 1. 背景

当前 `UcmPipelineStore` 通过 `PipelineStore.Stack()` 动态加载多个 `StoreV1`
实现，并通过 `store_backend` 将后加载的 Store 与前一层 Store 串联。现有
`Cache|Posix` 已实现以下能力：

- 上层 Store 先完成面向设备的写入。
- 后端 Posix 写入异步执行，不阻塞前台 Dump 任务完成。
- Load 在上层未命中时从后端读取。
- Lookup 优先查询上层，未命中部分再查询后端。

本方案新增 `YuanRongStore`，使用 YuanRong DataSystem SDK 的
`HeteroClient` H2D/D2H 接口承载 KV Cache 数据传输，并支持：

- `YuanRong`
- `YuanRong|Posix`

其中 `YuanRong|Posix` 的前台任务完成语义是：

> YuanRong 写入成功且后台 Posix 持久化任务已成功入队后，UCM Dump 任务
> 即完成；不等待 Posix 实际写盘完成。

## 2. 目标

1. 实现完整的 `StoreV1` 接口。
2. Dump 使用 `HeteroClient::MSetD2H`。
3. Load 使用 `HeteroClient::MGetH2D`。
4. Lookup 使用 `HeteroClient::Exist`。
5. 对外保持 `Load`、`Dump` 的异步 task handle 语义。
6. `YuanRong|Posix` 参考 CacheStore 的双队列模型实现异步落盘。
7. YuanRong 未命中时从 Posix 回源，但不回填 YuanRong。
8. 不重复实现 YuanRong SDK 已提供的 H2D/D2H、连接管理和共享内存管理。

## 3. 非目标

首期不包含以下能力：

- 不实现 YuanRong 数据淘汰策略。
- 不实现 Posix 数据回填 YuanRong。
- 不等待 Posix 持久化完成后再完成前台 Dump。
- 不修改 `StoreV1`、`PipelineStore` 或 Python connector 的公共接口。
- 不使用 `DevMSet`、`DevMGet` 等 D2D 接口。
- 不支持服务发现对象从 Python 配置直接传入，首期使用明确的 host/port。

## 4. 总体架构

```text
Python UcmPipelineStore
        |
        v
ucmpipelinestore.PipelineStore
        |
        v
YuanRongStore
  |         |
  |         +-- KVClient
  |              用于获取 YuanRong host ReadOnlyBuffer，
  |              供 Posix 后台落盘使用
  |
  +-- HeteroClient
  |      MSetD2H / MGetH2D / Exist
  |
  +-- TransManager
         |
         +-- LoadQueue
         |
         +-- DumpQueue
                |
                +-- waiting_：前台 YuanRong 写入
                |
                +-- dumping_：后台 Posix 持久化
```

`YuanRongStore` 是 pipeline 顶层时，`store_backend` 可以为空或指向 Posix：

```text
YuanRong
  YuanRongStore

YuanRong|Posix
  YuanRongStore
       |
       +-- store_backend -> PosixStore
```

Python builder 的构建顺序仍是先底层、后上层：

```python
pipeline.Stack("Posix", ".../posix/libposixstore.so", posix_config)
pipeline.Stack("YuanRong", ".../yuanrong/libyuanrongstore.so", config)
```

## 5. 文件结构

新增目录：

```text
ucm/store/yuanrong/
├── CMakeLists.txt
└── cc/
    ├── global_config.h
    ├── key_codec.h
    ├── trans_task.h
    ├── trans_manager.h
    ├── load_queue.h
    ├── load_queue.cc
    ├── dump_queue.h
    ├── dump_queue.cc
    └── yuanrong_store.cc
```

各文件职责：

- `global_config.h`
  - 保存 Store backend、设备、tensor size、队列深度和 YuanRong 连接配置。
- `key_codec.h`
  - 负责 `BlockId + shard_index` 与 YuanRong object key 之间的稳定转换。
- `trans_task.h`
  - 定义 LOAD/DUMP task 及递增 task handle。
- `trans_manager.h`
  - 基于 `Detail::TaskWrapper` 管理前台 task。
- `load_queue.*`
  - 在线程中执行同步 `MGetH2D`，并处理 Posix 回源。
- `dump_queue.*`
  - 在线程中执行同步 `MSetD2H`，并管理后台 Posix 落盘。
- `yuanrong_store.cc`
  - 实现 `StoreV1`、解析配置并初始化两个 YuanRong client。

不复用 CacheStore 的以下组件：

- `TransBuffer`
- `CopyStream`
- Cache buffer owner/ready 状态
- Cache 的 D2H/H2D scatter/gather

这些能力已由 YuanRong `HeteroClient` 负责。

## 6. 配置设计

新增以下配置项：

```yaml
store_pipeline: "YuanRong|Posix"

yuanrong_host: "127.0.0.1"
yuanrong_port: 9088
yuanrong_connect_timeout_ms: 3000
yuanrong_request_timeout_ms: 30000

yuanrong_token: ""
yuanrong_client_public_key: ""
yuanrong_client_private_key: ""
yuanrong_server_public_key: ""
yuanrong_access_key: ""
yuanrong_secret_key: ""
yuanrong_tenant_id: ""
yuanrong_enable_cross_node_connection: false
yuanrong_enable_exclusive_connection: false

yuanrong_waiting_queue_depth: 8192
yuanrong_running_queue_depth: 8192
yuanrong_persist_queue_depth: 8192
```

继续复用 UCM 通用配置：

```yaml
device_id: 0
tensor_size: 32768
shard_size: 2097152
block_size: 2097152
timeout_ms: 30000
cpu_affinity_cores: []
```

`tensor_size` 同时支持：

- 单一 `tensor_size`
- 可变长度 `tensor_size_list`

解析结果统一保存为 `std::vector<size_t> tensorSizes`。

`YuanRong|Posix` 首期要求：

```yaml
io_direct: false
```

同时要求：

```text
sum(tensor_size_list) == shard_size
```

PosixStore 按固定 `shard_size` 读写一个 shard，而 YuanRong object 的有效长度
是所有 blob size 之和。双层模式下二者不相等会导致 Posix 从
`ReadOnlyBuffer` 末尾继续读取，因此 Setup 阶段必须拒绝该配置。后续引入
bounce buffer 后，可以通过 padding 支持有效数据长度小于 `shard_size`。

原因是 YuanRong 当前没有承诺
`ReadOnlyBuffer::ImmutableData()` 返回的共享内存数据地址满足 Posix
`O_DIRECT` 的地址、长度和 offset 对齐要求。当前批量 SHM 子分配位置仅按
4 字节向上对齐，并且最终数据地址还会叠加 metadata offset。某些对象的地址
可能碰巧满足页对齐，但不能作为稳定接口契约使用。若用户配置
`io_direct: true`，`YuanRongStore::Setup()` 应返回 `InvalidParam`。

### 6.1 后续 O_DIRECT 适配

后续适配分为兼容方案和零拷贝方案。

#### 6.1.1 对齐 bounce buffer

第一阶段采用 YuanRongStore 自己管理的对齐 host buffer pool：

```text
ReadOnlyBuffer
  -> CPU memcpy 到 aligned host buffer
  -> Posix backend Dump
  -> backend Wait
  -> aligned buffer 归还 buffer pool
```

buffer pool 要求：

- 使用 `posix_memalign` 或等效接口分配。
- 地址按 PosixStore 要求的 direct I/O alignment 对齐。
- 单个 buffer 的分配长度按 alignment 向上取整。
- buffer 在 `backend_->Wait()` 完成前不得复用。
- pool 容量有上限，并与 persistence queue 深度联动。

提交 Posix 前校验：

```text
host_address % alignment == 0
write_length % alignment == 0
file_offset % alignment == 0
```

其中：

```text
file_offset = shard_index * shard_size
```

若 tensor size 总和小于对齐后的写入长度，bounce buffer 剩余区域必须清零。
Load 时仍只根据原始 tensor size 列表将有效数据写回 device，不使用 padding
区域。

该方案不增加第二次 D2H，但会增加一次 host-to-host memcpy，即从 YuanRong
SHM 数据地址复制到 YuanRongStore 管理的 aligned host buffer。它不要求修改
YuanRong SDK，适合作为首个支持 `io_direct=true` 的实现。

#### 6.1.2 YuanRong SHM 原生对齐

第二阶段在 YuanRong DataSystem 中保证 `MSetD2H` 创建的 host object 数据区
原生满足 direct I/O 对齐：

- SHM 分配起始地址按 alignment 对齐。
- metadata/lock 区域与 data 区域分离，或将 metadata size padding 到 alignment。
- `ReadOnlyBuffer::ImmutableData()` 返回的 data pointer 必须对齐。
- object 实际分配长度按 alignment 向上取整。
- YuanRong metadata 继续保存每个 blob 的真实长度，padding 不参与 H2D。
- SDK 对外提供明确的 alignment 能力查询或稳定 ABI 保证。

满足以下条件后，YuanRongStore 可以直接将共享内存地址传给 Posix：

```text
ReadOnlyBuffer::ImmutableData()
  -> Posix backend Dump
```

不再经过 bounce buffer，也不产生 host-to-host memcpy。

#### 6.1.3 自动选择

最终实现可在每个 persistence task 中动态选择：

```text
YuanRong SHM 地址、长度和文件 offset 全部对齐
  -> zero-copy direct I/O

任一条件不满足
  -> aligned bounce buffer
```

禁止在地址不对齐时静默关闭 `O_DIRECT`。是否使用 direct I/O 应在指标和日志中
可观察：

```text
yuanrong_persist_direct_zero_copy_total
yuanrong_persist_direct_bounce_total
yuanrong_persist_direct_bounce_bytes_total
```

## 7. Client 初始化

`YuanRongStore::Setup()` 使用相同的 `datasystem::ConnectOptions` 初始化：

```cpp
std::shared_ptr<datasystem::HeteroClient> heteroClient_;
std::shared_ptr<datasystem::KVClient> kvClient_;
```

初始化顺序：

1. 解析并校验配置。
2. 构造 `ConnectOptions`。
3. 创建并 `Init()` HeteroClient。
4. 创建并 `Init()` KVClient。
5. 启动 LoadQueue。
6. 启动 DumpQueue。

两个 client 必须连接同一个本地 YuanRong worker。这样
`KVClient::Get(...ReadOnlyBuffer...)` 才能优先走共享内存 mmap，而不是通过
TCP 重新传输完整数据。

析构顺序：

1. 停止接收新任务。
2. drain 前台 load/dump queue。
3. drain 后台 Posix dumping queue。
4. join 所有工作线程。
5. 调用 `KVClient::ShutDown()`。
6. 调用 `HeteroClient::ShutDown()`。
7. 释放 client。

## 8. Key 编码

YuanRong object key 必须同时包含：

- `Detail::BlockId`
- `shard.index`

采用格式：

```text
ucm_<32位小写十六进制BlockId>_<十进制shardIndex>
```

例如：

```text
ucm_0123456789abcdef0123456789abcdef_0
```

编码要求：

- 不依赖主机字节序。
- 不直接把二进制 BlockId 放入字符串。
- 相同 BlockId 和 shard index 始终生成相同 key。
- key 长度小于 YuanRong object key 限制。

与 CacheStore 保持一致，`Lookup(blockIds)` 只查询每个 block 的 shard 0 key：

```text
LookupKey(blockId) = Encode(blockId, 0)
```

其他 shard 仍使用各自的 `(BlockId, shardIndex)` key 存储。

## 9. Dump 详细流程

### 9.1 提交

```text
YuanRongStore::Dump(TaskDesc)
  -> TransManager::Submit(TransTask::DUMP)
  -> DumpQueue::Submit()
  -> 返回 task handle
```

`Dump()` 本身不调用 YuanRong SDK，不阻塞 D2H。

### 9.2 前置事件

如果 `TaskDesc.prerequisiteHandle != 0`，必须在调用 `MSetD2H` 前等待设备事件。

YuanRong HeteroClient 当前接口不接收 UCM 的事件 handle，因此 YuanRongStore
需要通过平台运行时完成同步屏障。可使用 UCM `Trans::Stream`：

```text
WaitEvent(prerequisiteHandle)
  -> Synchronized()
  -> MSetD2H()
```

不能只在 UCM stream 上异步提交 `WaitEvent` 后立即调用 `MSetD2H`，因为
HeteroClient 使用自己的传输流，两个 stream 之间没有天然顺序关系。该步骤只
负责保证源 device 数据已就绪，不由 YuanRongStore 自己执行 D2H。

### 9.3 构造 YuanRong 请求

每个 `Detail::Shard` 转换为一个 key 和一个 `DeviceBlobList`：

```cpp
DeviceBlobList list;
list.deviceIdx = config.deviceId;

for (size_t i = 0; i < shard.addrs.size(); ++i) {
    list.blobs.push_back({
        .pointer = shard.addrs[i],
        .size = config.tensorSizes[i],
    });
}
```

输入校验：

- task 不能为空。
- `deviceId >= 0`。
- 每个 shard 的 `addrs.size()` 必须等于 `tensorSizes.size()`。
- 所有地址必须非空。
- 每个 tensor size 必须大于 0。
- 单层 YuanRong 中，tensor size 总和不能超过 `shardSize`。
- `YuanRong|Posix` 中，tensor size 总和必须等于 `shardSize`。

### 9.4 写入 YuanRong

Dump worker 批量调用同步接口：

```cpp
auto status = heteroClient_->MSetD2H(keys, deviceBlobLists, setParam);
```

选择同步接口的原因：

- StoreV1 的异步语义已经由 `TransManager + DumpQueue` 提供。
- 同步接口返回时，YuanRong D2H 和对象发布已经完成。
- task handle、超时和任务清理由 UCM 统一管理。
- 避免同时维护 UCM task future 与 YuanRong shared_future。

YuanRong 对已存在 key 默认不覆盖。KV Cache 数据按 BlockId 标识且不可变，
因此重复 Dump 视为幂等成功。

### 9.5 写入结果校验

同步 `MSetD2H` 只返回一个 `Status`，没有逐 key failed list。该批量接口允许
部分成功，因此不能仅凭 `Status::OK()` 判断整个 UCM task 成功。

`MSetD2H` 返回成功后必须调用：

```cpp
std::vector<bool> exists;
auto status = heteroClient_->Exist(keys, exists);
```

校验要求：

- `Exist` 调用成功。
- `exists.size() == keys.size()`。
- 所有 `exists[i]` 都为 `true`。

任一 key 不可见时，前台 Dump task 失败，不提交 Posix 后台持久化任务。
重复 key 已经存在时 `Exist` 返回 true，仍按幂等成功处理。

### 9.6 后台 Posix 任务入队

对于单层 `YuanRong`：

```text
MSetD2H 成功且 Exist 校验全部 key 可见
  -> waiter->Done()
```

对于 `YuanRong|Posix`：

```text
MSetD2H 成功且 Exist 校验全部 key 可见
  -> 构造 PersistenceTask
  -> Push 到 dumping_
  -> waiter->Done()
```

`PersistenceTask` 保存：

```cpp
struct PersistenceTask {
    Detail::TaskHandle foregroundTaskId;
    std::vector<std::string> keys;
    std::vector<Detail::BlockId> owners;
    std::vector<size_t> shardIndexes;
};
```

`dumping_` 使用有界阻塞队列。队列满时 Dump worker 等待可用槽位，不静默
丢弃 Posix 数据。这样前台任务完成表示后台持久化任务至少已被可靠接收。

前台 task 不等待：

- `KVClient::Get`
- `backend_->Dump`
- `backend_->Wait`
- Posix 实际落盘

### 9.7 后台 Posix 落盘

后台线程消费 `dumping_`：

```text
PersistenceTask
  -> KVClient::Get(keys, ReadOnlyBuffers)
  -> 构造 backend TaskDesc
  -> backend_->Dump()
  -> backend_->Wait()
  -> 释放 ReadOnlyBuffers
```

必须使用以下重载：

```cpp
KVClient::Get(
    const std::vector<std::string>& keys,
    std::vector<Optional<ReadOnlyBuffer>>& buffers,
    int32_t timeoutMs);
```

禁止使用 `std::vector<std::string>` 重载，因为该重载会复制完整数据。

构造 Posix backend shard：

```cpp
Detail::Shard backendShard;
backendShard.owner = owners[i];
backendShard.index = shardIndexes[i];
backendShard.addrs = {
    const_cast<void*>(readOnlyBuffers[i]->ImmutableData())
};
```

Posix 的 `tensor_size` 配置为 `shard_size`，因此一个连续
`ReadOnlyBuffer` 对应一个 backend shard。

`ReadOnlyBuffer` 必须保存在后台任务上下文中，直到
`backend_->Wait(handle)` 返回，确保 Posix 异步读取期间共享内存地址有效。

后台 Posix 失败：

- 记录 error 日志。
- 增加持久化失败指标。
- 不修改已经完成的前台 UCM task。
- 继续处理后续持久化任务。

## 10. Load 详细流程

### 10.1 提交

```text
YuanRongStore::Load(TaskDesc)
  -> TransManager::Submit(TransTask::LOAD)
  -> LoadQueue::Submit()
  -> 返回 task handle
```

### 10.2 YuanRong H2D

Load worker 将 shard 转换成 key 和 `DeviceBlobList`，调用：

```cpp
heteroClient_->MGetH2D(
    keys,
    deviceBlobLists,
    failedKeys,
    config.timeoutMs);
```

### 10.3 Posix 回源

`failedKeys` 是 YuanRong 未命中或读取失败的 shard：

- 无 backend：任一 failed key 导致前台 task 失败。
- 有 backend：只为 failed keys 对应的 shard 构造 backend TaskDesc。

```text
YuanRong 命中 shard
  -> 数据已 H2D

YuanRong 未命中 shard
  -> backend_->Load(failedShardDesc)
  -> backend_->Wait()
  -> 不回填 YuanRong
```

Load task 在 YuanRong 命中部分和 Posix 回源部分都完成后才完成。

如果 YuanRong 返回非 NotFound 的系统错误，默认整批 Load 失败，不使用 Posix
掩盖连接、鉴权或运行时异常。只有明确的未命中 key 才允许回源。

## 11. Lookup 和 LookupOnPrefix

### 11.1 Lookup

1. 为每个 BlockId 生成 shard 0 key。
2. 调用 `HeteroClient::Exist(keys, exists)`。
3. YuanRong 命中的位置直接返回 `true`。
4. 若有 backend，将 YuanRong 未命中的 BlockId 批量传给
   `backend_->Lookup()`。
5. 将 backend 结果合并回原顺序。

```text
result[i] = yuanrongExists[i] || backendExists[i]
```

如果 YuanRong `Exist` 返回系统错误，Lookup 返回错误，不自动忽略 YuanRong
异常。

### 11.2 LookupOnPrefix

复用 Lookup 结果，从索引 0 开始查找首个 `false`：

```text
全部命中 -> num - 1
首个未命中位于 i -> i - 1
第一个即未命中 -> -1
空输入 -> -1
```

不需要 YuanRong 额外提供 prefix 接口。

### 11.3 Prefetch

首期 `Prefetch` 保持 no-op，与 CacheStore 当前行为一致。

## 12. Task 与队列模型

`TransManager` 参考 CacheStore：

```cpp
class TransManager
    : public Detail::TaskWrapper<TransTask, Detail::TaskHandle> {
    LoadQueue loadQueue_;
    DumpQueue dumpQueue_;
};
```

前台 task 状态：

- `Submit()` 创建 task handle。
- `Check()` 查询 waiter 是否完成。
- `Wait()` 等待 waiter，并在结束后清理 task。
- 超时后 task 标记失败。

使用以下队列：

```text
LoadQueue.waiting_
DumpQueue.waiting_
DumpQueue.dumping_
```

所有队列必须有容量限制，禁止无限增长。

队列满处理：

- load/dump waiting queue 满：当前前台 task 失败。
- dumping queue 满：阻塞 Dump worker，直到可入队或 Store 开始退出。
- Store 退出期间拒绝新任务，并唤醒所有阻塞线程。

## 13. 错误处理

### 13.1 前台失败

以下错误必须反映到 UCM task：

- 配置错误。
- HeteroClient/KVClient 初始化失败。
- prerequisite event 等待失败。
- DeviceBlobList 构造失败。
- `MSetD2H` 失败。
- `MSetD2H` 后的逐 key Exist 校验失败。
- `MGetH2D` 系统错误。
- 无 backend 时的 YuanRong Load miss。
- Posix 回源 Load 提交或等待失败。
- 前台队列满或 task 超时。

### 13.2 后台持久化失败

以下错误只记录日志和指标：

- `KVClient::Get(ReadOnlyBuffer)` 失败。
- ReadOnlyBuffer 数量或大小不匹配。
- `backend_->Dump()` 提交失败。
- `backend_->Wait()` 失败或超时。

原因是这些错误发生时，YuanRong 已成功写入，前台 task 已完成。

### 13.3 部分成功

对批量 H2D/D2H：

- 必须检查 `Status`。
- MGetH2D 必须检查 failed key 列表。
- MSetD2H 必须在成功后通过 Exist 校验全部 key。
- 不能将“至少一个成功”当作整批 task 成功。
- 日志中输出失败数量和首个失败 key，不输出大批完整 key 列表。

## 14. 线程安全与生命周期

`StoreV1` 要求所有公共方法线程安全。

设计要求：

- HeteroClient、KVClient 的调用通过队列 worker 串行化，或确认 SDK 支持并发后
  配置固定 worker 数。
- task map 由 `TaskWrapper` 保护。
- dumping queue 中的 `ReadOnlyBuffer` 生命周期覆盖 Posix task。
- shutdown 前必须 drain 或明确失败所有未完成前台 task。
- backend Store 生命周期由 PipelineStore 保证晚于 YuanRongStore 析构。
- YuanRongStore 析构时不得再提交新的 backend task。

首期使用：

- 一个 Load worker。
- 一个 Dump worker。
- 一个 Posix persistence worker。

先保证顺序和生命周期正确，再根据性能数据增加并发。

## 15. Pipeline 注册

修改 `ucm/store/pipeline/connector.py`，新增：

```python
def _yuanrong_pipeline_builder(config, pipeline):
    pipeline.Stack(
        "YuanRong",
        str(store_dir / "yuanrong/libyuanrongstore.so"),
        config,
    )


def _yuanrong_posix_pipeline_builder(config, pipeline):
    posix_config = copy.deepcopy(config)
    posix_config["tensor_size"] = config["shard_size"]
    posix_config["io_direct"] = False

    pipeline.Stack(
        "Posix",
        str(store_dir / "posix/libposixstore.so"),
        posix_config,
    )
    pipeline.Stack(
        "YuanRong",
        str(store_dir / "yuanrong/libyuanrongstore.so"),
        config,
    )
```

注册：

```python
UcmPipelineStoreBuilder.register("YuanRong", _yuanrong_pipeline_builder)
UcmPipelineStoreBuilder.register(
    "YuanRong|Posix",
    _yuanrong_posix_pipeline_builder,
)
```

## 16. 构建与安装

修改：

```text
ucm/store/CMakeLists.txt
```

增加：

```cmake
add_subdirectory(yuanrong)
```

`ucm/store/yuanrong/CMakeLists.txt`：

- `find_path()` 查找 `datasystem/hetero_client.h`。
- `find_library()` 查找 `libdatasystem.so`。
- 构建 `yuanrongstore` shared library。
- 链接：
  - `storeintf`
  - `infra_logger`
  - `metrics`
  - `trans`
  - `datasystem`
- 安装为：

```text
ucm/store/yuanrong/libyuanrongstore.so
```

- 设置运行时 RPATH，使其能找到 YuanRong SDK 的 `libdatasystem.so` 及其依赖。

导出工厂符号：

```cpp
extern "C" UC::StoreV1* MakeYuanRongStore()
{
    return new UC::YuanRongStore::YuanRongStore();
}
```

## 17. 指标

首期指标：

```text
yuanrong_lookup_duration_ms
yuanrong_lookup_blocks_total
yuanrong_lookup_hit_blocks_total

yuanrong_load_duration_ms
yuanrong_load_blocks_total
yuanrong_load_backend_blocks_total
yuanrong_load_errors_total

yuanrong_dump_duration_ms
yuanrong_dump_blocks_total
yuanrong_dump_errors_total

yuanrong_persist_queue_depth
yuanrong_persist_duration_ms
yuanrong_persist_blocks_total
yuanrong_persist_errors_total
```

前台 Dump duration 只统计到 YuanRong 写入及持久化任务入队完成，不包含 Posix
实际写盘时间。

## 18. 测试方案

### 18.1 单元测试

1. KeyCodec
   - BlockId 编码稳定。
   - 不同 shard index 生成不同 key。
   - shard 0 Lookup key 与 Dump key 一致。

2. 配置
   - 缺少 host/port 失败。
   - 无效 device id 失败。
   - tensor size 与地址数量不匹配失败。
   - `YuanRong|Posix + io_direct=true` 失败。

3. Lookup
   - YuanRong 全命中。
   - YuanRong 部分命中，Posix 补齐。
   - YuanRong 与 Posix 均未命中。
   - Prefix 在首个 miss 处停止。

4. Dump
   - MSetD2H 成功后 task 完成。
   - MSetD2H 失败后 task 失败。
   - MSetD2H 部分成功但 Exist 校验不完整时 task 失败。
   - 单层 YuanRong 不调用 KVClient/Get/backend。
   - 双层模式成功入队后 task 完成，不等待 backend Wait。
   - 后台 Get 或 Posix 失败不修改前台 task。
   - ReadOnlyBuffer 在 backend Wait 前不释放。

5. Load
   - YuanRong 全命中。
   - 部分 miss 只将失败 shard 交给 Posix。
   - Posix 回源后不调用 MSetD2H 回填。
   - 无 backend 时 miss 导致 task 失败。

6. Shutdown
   - 有前台任务时安全退出。
   - 有后台 Posix 任务时安全 drain。
   - 队列满时退出不会死锁。

### 18.2 集成测试

新增类似 CacheStore e2e 的测试：

```text
ucm/store/test/e2e/yuanrong_store_test.py
ucm/store/test/e2e/yuanrong_on_posix_test.py
```

覆盖：

- Dump -> Lookup -> Load 数据一致性。
- scheduler 实例 `device_id=-1` 只执行 Lookup。
- worker 实例带 `device_id` 执行 H2D/D2H。
- YuanRong miss 后从 Posix Load。
- YuanRong 写完成时前台 task 已完成，Posix 可稍后查询到。
- 重复 Dump 幂等。
- 多 block、多 shard 和可变 tensor size。

### 18.3 性能验证

重点测量：

- `MSetD2H` 延迟。
- `MSetD2H -> KVClient::Get(ReadOnlyBuffer)` 延迟。
- KVClient Get 是否命中 SHM 路径。
- 首次 mmap 与后续 mmap 的差异。
- Posix 后台吞吐是否落后于 YuanRong Dump。
- dumping queue 的稳定深度和峰值。

## 19. 验收标准

1. `store_pipeline: "YuanRong"` 可以完成 Lookup、Load、Dump。
2. `store_pipeline: "YuanRong|Posix"` 可以完成：
   - YuanRong 优先 Lookup。
   - YuanRong miss 后 Posix 回源 Load。
   - YuanRong Dump 成功后前台 task 完成。
   - Posix 在后台完成持久化。
3. Dump 只执行一次设备到 host 的数据传输。
4. 后台 Posix 写入使用 YuanRong `ReadOnlyBuffer`，不复制到
   `std::string`。
5. Posix 后台失败不会导致已经成功的 YuanRong task 变为失败。
6. 所有队列有容量限制，退出过程无死锁、无 use-after-free。
7. 单元测试和新增 e2e 测试通过。

## 20. 最终设计决策摘要

- 使用 `MSetD2H/MGetH2D`，不使用 D2D 接口。
- StoreV1 异步语义由 UCM queue/task framework 提供，YuanRong SDK 使用同步接口。
- Dump 参考 CacheStore 的 `waiting_ + dumping_` 两阶段模型。
- YuanRong 写入成功并成功入持久化队列后，前台 Dump task 完成。
- Posix 后台通过 `KVClient::Get(ReadOnlyBuffer)` 获取共享内存地址。
- ReadOnlyBuffer 持有到 Posix Wait 完成。
- Load miss 从 Posix 回源，不回填 YuanRong。
- Lookup 只检查 shard 0，与 CacheStore 当前行为保持一致。
- `YuanRong|Posix` 首期不支持 `io_direct=true`。
