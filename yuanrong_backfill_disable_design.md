# YuanRong｜Posix Backfill 可关闭方案

## 1. 背景

`YuanRong|Posix` 采用分层存储：YuanRong 作为优先访问层，Posix 作为后端持久化层。加载 KV Cache 时，系统优先从 YuanRong 加载；YuanRong 未命中的数据从 Posix 恢复到 Host Buffer，再复制到 Device。当前实现还会将这批从 Posix 恢复的数据异步回填到 YuanRong，提升后续相同数据的访问性能。

Backfill 会占用额外的 YuanRong 容量、Host Buffer、CPU 和网络带宽。在部分业务中，从 Posix 恢复的数据重用率较低，或者用户希望将 YuanRong 容量留给主动写入的数据，此时需要关闭回填，同时保留 YuanRong 查询、YuanRong 命中加载以及 Posix 冷数据恢复能力。

本方案使用现有参数 `yuanrong_backfill_worker_count` 控制功能开关，不新增独立的布尔参数：

```yaml
# 0：关闭 Posix -> YuanRong backfill
# >0：开启 backfill，数值表示 backfill worker 并发数
yuanrong_backfill_worker_count: 0
```

默认值保持为 `1`，已有配置和现有行为不变。

## 2. 现状

### 2.1 当前加载流程

`YuanRong|Posix` 的加载流程如下：

1. 调用 YuanRong `Exist` 判断对象是否存在。
2. YuanRong 命中的对象通过 `MGetH2D`/`AsyncMGetH2D` 加载到 Device。
3. YuanRong 未命中的对象通过 Posix 加载到 Host Buffer。
4. Host Buffer 中的数据通过 H2D copy 写入 Device。
5. H2D 完成后，无条件向 `BackfillQueue` 提交任务。
6. Backfill worker 通过 YuanRong `MCreate` 和 `MSet` 将数据异步写回 YuanRong。

Backfill 不在主加载结果的关键路径上；队列满时会丢弃回填任务，不影响本次 Posix 恢复和 H2D 的成功结果。

### 2.2 当前配置限制

当前相关参数为：

| 参数 | 默认值 | 当前含义 |
| --- | ---: | --- |
| `yuanrong_backfill_worker_count` | `1` | Backfill worker 数量，当前要求大于 `0` |
| `yuanrong_backfill_queue_depth` | `128` | Backfill 等待队列深度，当前要求大于 `0` |

在 worker 模式且配置了 Posix backend 时，`yuanrong_backfill_worker_count: 0` 会被配置校验拒绝，因此目前无法通过配置关闭 backfill。

### 2.3 不能只放开配置校验的原因

仅删除 `worker_count > 0` 校验会引入两个问题：

1. `BackfillQueue::Setup` 会创建零个 worker，但 `Submit` 仍可将任务放入队列。这些任务无人消费，并持续持有 Host Buffer，直到队列满或组件关闭。
2. `DeriveYuanRongHostBufferCount` 当前将 `backfillWorkerCount == 0` 视为非法输入并返回 `0`。自动推导 Host Buffer 数量时，这会导致 `YuanRong|Posix` 初始化失败。

因此，`0` 必须在配置校验、队列生命周期、任务提交和 Host Buffer 容量推导中具有一致的“关闭”语义。

## 3. Use Case

### 3.1 低重用冷数据

从 Posix 恢复的数据后续很少再次访问。关闭 backfill 可避免冷数据进入 YuanRong，减少缓存污染和无效写入。

### 3.2 YuanRong 容量受限

业务希望优先保留主动 Dump 到 YuanRong 的对象，不希望 Posix 恢复的数据继续占用 YuanRong 内存容量。

### 3.3 降低恢复路径的后台开销

在 Posix 恢复量较大时，backfill 会产生额外的内存复制、CPU 消耗和 YuanRong 网络写流量。关闭后可降低后台资源竞争，使资源集中在当前请求的 Posix 恢复和 H2D 上。

### 3.4 性能测试与问题定位

需要独立评估 Posix 恢复性能，或排除 backfill 对性能、容量及 YuanRong 写入错误的影响时，可临时将 worker 数量设置为 `0`。

### 3.5 非目标场景

关闭 backfill 不代表禁用 YuanRong，也不改变以下行为：

- 加载时仍会查询 YuanRong，并优先使用 YuanRong 命中数据。
- YuanRong 未命中时仍会从 Posix 恢复并完成 H2D。
- 正常 Dump/持久化流程保持不变。
- `YuanRong|Posix` 对 YuanRong 服务和 SDK 的依赖保持不变。

如果目标是完全绕过 YuanRong，应选择其他 store pipeline，而不是使用该参数。

## 4. 方案

### 4.1 参数语义

复用 `yuanrong_backfill_worker_count`：

| 配置值 | 行为 |
| ---: | --- |
| `0` | 禁用 Posix 恢复数据到 YuanRong 的 backfill |
| `N > 0` | 启用 backfill，并创建 `N` 个 worker |

兼容性约定：

- 默认值继续为 `1`，未显式配置时行为不变。
- 开启 backfill 时，`yuanrong_backfill_queue_depth` 必须大于 `0`。
- 关闭 backfill 时，`yuanrong_backfill_queue_depth` 不参与运行，也不要求用户额外修改；建议忽略其取值。
- 不新增 `yuanrong_enable_backfill`，避免两个参数组合产生冲突状态。

### 4.2 配置校验

调整 `YuanRongStore::CheckConfig`：

- 删除 `yuanrong_backfill_worker_count == 0` 的报错。
- 仅当 `yuanrong_backfill_worker_count > 0` 时，校验 `yuanrong_backfill_queue_depth > 0`。
- 其他 `YuanRong|Posix` 参数校验保持不变。

伪代码如下：

```cpp
if (config.backfillWorkerCount > 0 && config.backfillQueueDepth == 0) {
    return Status::InvalidParam("yuanrong_backfill_queue_depth must be greater than 0");
}
```

### 4.3 BackfillQueue 生命周期

调整 `LoadQueue::Setup`：

- Posix backend 存在时，Host Buffer Pool 仍需正常初始化。
- 仅当 `backfillWorkerCount > 0` 时调用 `BackfillQueue::Setup`。
- `backfillWorkerCount == 0` 时不创建 worker，也不初始化回填队列。

建议同时在 `BackfillQueue` 内增加防御性保护：未启动 worker 时拒绝 `Submit`，避免未来新增调用点绕过 `LoadQueue` 的开关判断。该保护只作为兜底，正常关闭路径不应调用 `Submit`。

### 4.4 Posix 恢复完成后的处理

调整 `LoadQueue::FinalizeHostBatch`：

- H2D copy 和 stream synchronize 成功后判断 `backfillWorkerCount`。
- 大于 `0`：保持现有行为，将 keys 和 Host Buffer 所有权转移给 `BackfillQueue`。
- 等于 `0`：不创建、不提交 `BackfillTask`，立即释放本批 Host Buffer，使其返回 Host Buffer Pool。

伪代码如下：

```cpp
if (config_.backfillWorkerCount > 0) {
    backfillQueue_.Submit(
        BackfillTask{std::move(batch.keys), std::move(batch.hostBuffers)});
} else {
    batch.hostBuffers.clear();
}
```

这里应显式释放 `hostBuffers`，而不是仅依赖 `HostBatch` 最终析构，以便并行准备下一恢复批次时及时归还 Buffer，降低 Buffer Pool 等待和超时风险。

### 4.5 Host Buffer 数量推导

当前自动容量目标为：

```text
目标批次数 = loadWorkerCount × 2 + backfillWorkerCount
```

其中每个 load worker 预留当前批和 lookahead 批，每个 backfill worker 预留一个正在处理的批。关闭 backfill 后，应自然退化为：

```text
目标批次数 = loadWorkerCount × 2
```

调整 `DeriveYuanRongHostBufferCount`：

- 不再将 `backfillWorkerCount == 0` 判定为非法。
- 保留 `objectSize == 0`、`recoveryBatchSize == 0`、`loadWorkerCount == 0` 或 `capacityBytes == 0` 时返回 `0` 的行为。
- 继续根据 `hostBufferCapacityGb` 限制最终完整批次数。
- 保留整数溢出保护。

例如，在 object size 为 4 MiB、batch size 为 32、load worker 为 4、容量为 8 GiB 时：

- Backfill worker 为 `1`：目标为 9 批，即 288 个 Buffer。
- Backfill worker 为 `0`：目标为 8 批，即 256 个 Buffer。

### 4.6 日志与示例配置

- 启动日志继续输出 `BackfillWorkerCount`；值为 `0` 时增加明确日志，例如 `YuanRong backfill disabled`。
- 关闭模式下不输出“async backfill submitted”日志。
- 在 `examples/ucm_yuanrong_config.yaml` 中补充注释，说明 `0` 表示关闭，默认示例值仍可保持 `1`。

### 4.7 涉及文件

预计修改范围：

| 文件 | 修改内容 |
| --- | --- |
| `ucm/store/yuanrongstore/cc/yuanrong_store.cc` | 配置校验和启动日志 |
| `ucm/store/yuanrongstore/cc/load_queue.cc` | 按开关初始化队列、条件提交、及时释放 Host Buffer |
| `ucm/store/yuanrongstore/cc/backfill_queue.cc/.h` | 可选的零 worker 防御性保护和可测试状态 |
| `ucm/store/yuanrongstore/cc/yuanrong_helper.h` | 支持零 backfill worker 的 Host Buffer 数量推导 |
| `ucm/store/test/case/yuanrong/yuanrong_store_unit_test.cc` | 容量推导及配置语义单元测试 |
| `examples/ucm_yuanrong_config.yaml` | 参数语义注释 |

## 5. 测试

### 5.1 配置测试

| 场景 | 配置 | 预期结果 |
| --- | --- | --- |
| 默认行为 | 不配置 worker count | 使用默认值 `1`，backfill 开启 |
| 关闭 backfill | worker count = `0` | 配置校验和初始化成功 |
| 正常开启 | worker count = `1` 或更大 | 配置校验和初始化成功 |
| 开启但队列无容量 | worker count > `0`，queue depth = `0` | 配置校验失败 |
| 关闭且队列深度为零 | worker count = `0`，queue depth = `0` | 配置校验成功，queue depth 被忽略 |

### 5.2 Host Buffer 推导单元测试

扩展 `DeriveYuanRongHostBufferCount` 测试：

- `backfillWorkerCount = 0` 时返回 `loadWorkerCount × 2` 个完整批次对应的 Buffer 数量。
- `backfillWorkerCount > 0` 时保持现有结果，验证兼容性。
- 容量不足一个完整批次时仍返回 `0`。
- object size、recovery batch size、load worker count 或 capacity 为 `0` 时仍返回 `0`。
- 覆盖大数输入，确认溢出保护不回退。

### 5.3 BackfillQueue 单元测试

- worker count 为 `0` 时不创建线程。
- 禁用状态下 `Submit` 返回失败且不持有传入的 Host Buffer。
- worker count 大于 `0` 时任务仍可正常入队和消费。
- 队列满时保持现有丢弃行为。
- `Close` 在启用和禁用状态下均可重复调用，不死锁、不崩溃。

### 5.4 加载流程测试

使用 mock/stub YuanRong client 和 Posix backend 覆盖：

1. **Backfill 关闭、YuanRong 未命中**
   - 从 Posix 成功加载。
   - H2D 成功。
   - 不调用 YuanRong `MCreate`/`MSet`。
   - Host Buffer 在当前批完成后归还池中。

2. **Backfill 开启、YuanRong 未命中**
   - 从 Posix 成功加载并完成 H2D。
   - 提交一次 backfill。
   - YuanRong `MCreate`/`MSet` 调用行为与当前实现一致。

3. **YuanRong 命中**
   - 不论 backfill 是否开启，均直接执行 YuanRong H2D。
   - 不访问 Posix，不提交 backfill。

4. **混合命中**
   - YuanRong 命中部分从 YuanRong 加载，未命中部分从 Posix 恢复。
   - 关闭时未命中部分不回填；开启时只回填未命中部分。

5. **多批次及并发恢复**
   - backfill 关闭时连续处理多批次，确认 Host Buffer 无泄漏。
   - Buffer Pool 容量按 `loadWorkerCount × 2` 配置时无死锁和非预期超时。

### 5.5 回归与验收

- 运行 YuanRong 相关单元测试和 store 测试。
- 使用默认配置执行现有 `YuanRong|Posix` 场景，确认 backfill 行为和性能没有功能性回退。
- 使用 `yuanrong_backfill_worker_count: 0` 执行端到端冷加载：首次和再次加载均可从 Posix 恢复，YuanRong 中不新增对应回填对象。
- 检查进程线程列表，关闭模式下不存在 backfill worker。
- 检查日志及 YuanRong 指标，关闭模式下没有 backfill submit、`MCreate` 和 `MSet` 活动。
- 对比关闭前后的 Host Buffer 占用，确认容量推导减少一个 backfill 批次且运行中无 Buffer 泄漏。

## 6. 验收标准

方案完成后应满足：

1. `yuanrong_backfill_worker_count: 0` 可正常启动 `YuanRong|Posix`。
2. YuanRong 命中加载与 Posix 冷恢复能力不受影响。
3. 从 Posix 恢复的数据不再提交或写入 YuanRong。
4. 禁用状态下不创建 backfill worker，不积压回填任务，不额外持有 Host Buffer。
5. `yuanrong_backfill_worker_count > 0` 的现有行为保持兼容。
6. 自动推导的 Host Buffer 数量在启用和禁用模式下均合理，且无死锁、泄漏或非预期超时。

