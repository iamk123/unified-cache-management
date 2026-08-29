/**
 * MIT License
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include "datasystem/hetero_client.h"
#include "datasystem/kv_client.h"
#include "logger/logger.h"
#include "task_manager.h"
#include "time/now_time.h"
#include "ucmstore_v1.h"
#include "yuanrong_config.h"
#include "yuanrong_helper.h"

namespace UC::YuanRongStore {

class YuanRongStore : public StoreV1 {
    Config config_{};
    std::shared_ptr<datasystem::HeteroClient> heteroClient_;
    std::shared_ptr<datasystem::KVClient> kvClient_;
    TaskManager taskManager_;
    bool taskManagerEnabled_{false};
    std::mutex registerMtx_;
    std::unordered_map<void*, size_t> registered_;

public:
    ~YuanRongStore() override = default;

    Status Setup(const Detail::Dictionary& input) override
    {
        config_ = ParseConfig(input);
        auto s = CheckConfig(config_);
        if (s.Failure()) { return s; }
        s = ResolveDeviceMemoryPreRegistration(config_, std::getenv("DS_RH2D_LINK_TYPE"));
        if (s.Failure()) { return s; }

        datasystem::ConnectOptions options;
        options.host = config_.host;
        options.port = config_.port;
        options.requestTimeoutMs = static_cast<int32_t>(config_.timeoutMs);
        options.enableRemoteH2D = config_.enableRemoteH2D;

        heteroClient_ = std::make_shared<datasystem::HeteroClient>(options);
        auto heteroStatus = heteroClient_->Init();
        if (heteroStatus.IsError()) {
            return Status::Error("failed to initialize YuanRong HeteroClient: " +
                                 heteroStatus.ToString());
        }

        kvClient_ = std::make_shared<datasystem::KVClient>(options);
        auto kvStatus = kvClient_->Init();
        if (kvStatus.IsError()) {
            return Status::Error("failed to initialize YuanRong KVClient: " + kvStatus.ToString());
        }

        if (config_.deviceId >= 0) {
            s = RegisterKvBuffers(config_);
            if (s.Failure()) { return s; }
            s = taskManager_.Setup(config_, heteroClient_, kvClient_);
            if (s.Failure()) { return s; }
            taskManagerEnabled_ = true;
        }
        ShowConfig(config_);
        return Status::OK();
    }

    std::string Readme() const override { return "YuanRongStore"; }

    Expected<std::vector<uint8_t>> Lookup(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return std::vector<uint8_t>{}; }
        auto lookupResult = LookupYuanRong(blocks, num);
        if (!lookupResult) { return lookupResult.Error(); }
        const auto& exists = lookupResult.Value();

        std::vector<uint8_t> result(num, 0);
        std::vector<Detail::BlockId> missBlocks;
        std::vector<size_t> missIndexes;
        missBlocks.reserve(num);
        missIndexes.reserve(num);
        for (size_t i = 0; i < num; ++i) {
            result[i] = exists[i] ? 1 : 0;
            if (!exists[i]) {
                missBlocks.push_back(blocks[i]);
                missIndexes.push_back(i);
            }
        }
        if (missBlocks.empty() || config_.storeBackend == nullptr) { return result; }

        auto backendStart = NowTime::Now();
        auto backendResult = config_.storeBackend->Lookup(missBlocks.data(), missBlocks.size());
        auto backendEnd = NowTime::Now();
        if (!backendResult) { return backendResult.Error(); }
        const auto& backendExists = backendResult.Value();
        if (backendExists.size() != missBlocks.size()) {
            return Status::Error("backend Lookup returned an unexpected result size");
        }
        for (size_t i = 0; i < missIndexes.size(); ++i) {
            result[missIndexes[i]] = backendExists[i];
        }
        UC_DEBUG("YuanRong Lookup queried Posix misses={}/{}, cost={:.3f}ms.", missBlocks.size(),
                 num, (backendEnd - backendStart) * 1e3);
        return result;
    }

    Expected<ssize_t> LookupOnPrefix(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return static_cast<ssize_t>(-1); }
        auto lookupResult = LookupYuanRong(blocks, num);
        if (!lookupResult) { return lookupResult.Error(); }
        const auto& exists = lookupResult.Value();

        std::vector<Detail::BlockId> missBlocks;
        std::vector<size_t> missIndexes;
        missBlocks.reserve(num);
        missIndexes.reserve(num);
        for (size_t i = 0; i < num; ++i) {
            if (!exists[i]) {
                missBlocks.push_back(blocks[i]);
                missIndexes.push_back(i);
            }
        }
        if (missBlocks.empty()) { return static_cast<ssize_t>(num) - 1; }
        if (config_.storeBackend == nullptr) {
            return static_cast<ssize_t>(missIndexes.front()) - 1;
        }

        auto backendStart = NowTime::Now();
        auto backendResult =
            config_.storeBackend->LookupOnPrefix(missBlocks.data(), missBlocks.size());
        auto backendEnd = NowTime::Now();
        if (!backendResult) { return backendResult.Error(); }
        ssize_t result = -1;
        auto status = ResolveTieredPrefixHit(num, missIndexes, backendResult.Value(), result);
        if (status.Failure()) { return status; }
        UC_DEBUG("YuanRong LookupOnPrefix queried Posix misses={}/{}, cost={:.3f}ms, result={}.",
                 missBlocks.size(), num, (backendEnd - backendStart) * 1e3, result);
        return result;
    }

    Expected<ssize_t> LookupOnReverse(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return static_cast<ssize_t>(-1); }
        auto lookupResult = LookupYuanRong(blocks, num);
        if (!lookupResult) { return lookupResult.Error(); }
        const auto& exists = lookupResult.Value();

        ssize_t yuanRongHit = -1;
        for (size_t i = num; i > 0; --i) {
            if (exists[i - 1]) {
                yuanRongHit = static_cast<ssize_t>(i - 1);
                break;
            }
        }
        if (yuanRongHit == static_cast<ssize_t>(num) - 1 || config_.storeBackend == nullptr) {
            return yuanRongHit;
        }

        const auto backendStartIndex = static_cast<size_t>(yuanRongHit + 1);
        auto backendStart = NowTime::Now();
        auto backendResult = config_.storeBackend->LookupOnReverse(blocks + backendStartIndex,
                                                                   num - backendStartIndex);
        auto backendEnd = NowTime::Now();
        if (!backendResult) { return backendResult.Error(); }

        ssize_t result = -1;
        auto status = ResolveTieredReverseHit(num, yuanRongHit, backendResult.Value(), result);
        if (status.Failure()) { return status; }
        UC_DEBUG("YuanRong LookupOnReverse queried Posix misses={}/{}, cost={:.3f}ms, result={}.",
                 num - backendStartIndex, num, (backendEnd - backendStart) * 1e3, result);
        return result;
    }

    void Prefetch(const Detail::BlockId* blocks, size_t num) override
    {
        (void)blocks;
        (void)num;
    }

    Expected<Detail::TaskHandle> Load(Detail::TaskDesc task) override
    {
        if (!taskManagerEnabled_) { return Status::Unsupported(); }
        return taskManager_.Submit(TransTask{TransTask::Type::LOAD, std::move(task)});
    }

    Expected<Detail::TaskHandle> Dump(Detail::TaskDesc task) override
    {
        if (!taskManagerEnabled_) { return Status::Unsupported(); }
        return taskManager_.Submit(TransTask{TransTask::Type::DUMP, std::move(task)});
    }

    Expected<bool> Check(Detail::TaskHandle taskId) override
    {
        if (!taskManagerEnabled_) { return Status::Unsupported(); }
        return taskManager_.Check(taskId);
    }

    Status Wait(Detail::TaskHandle taskId) override
    {
        if (!taskManagerEnabled_) { return Status::Unsupported(); }
        return taskManager_.Wait(taskId);
    }

private:
    Status RegisterKvBuffers(const Config& config)
    {
        if (!config.enableDeviceMemoryPreRegistration || config.gpuKvBufferAddrs.empty()) {
            return Status::OK();
        }
        if (heteroClient_ == nullptr) {
            return Status::Error("YuanRong HeteroClient is not initialized");
        }

        std::lock_guard<std::mutex> lock(registerMtx_);
        std::vector<void*> addrs;
        std::vector<uint64_t> sizes;
        addrs.reserve(config.gpuKvBufferAddrs.size());
        sizes.reserve(config.gpuKvBufferSizes.size());
        for (size_t i = 0; i < config.gpuKvBufferAddrs.size(); ++i) {
            auto* addr = reinterpret_cast<void*>(config.gpuKvBufferAddrs[i]);
            const auto size = config.gpuKvBufferSizes[i];
            auto iter = registered_.find(addr);
            if (iter != registered_.end() && iter->second >= size) { continue; }
            addrs.push_back(addr);
            sizes.push_back(static_cast<uint64_t>(size));
        }
        if (addrs.empty()) { return Status::OK(); }

        auto status = heteroClient_->PreRegisterDeviceMemory(addrs, sizes);
        if (status.IsError()) {
            UC_ERROR("YuanRong PreRegisterDeviceMemory failed: buffers={}, error={}", addrs.size(),
                     status.ToString());
            return Status::Error("YuanRong PreRegisterDeviceMemory failed: " + status.ToString());
        }
        uint64_t totalSize = 0;
        for (size_t i = 0; i < addrs.size(); ++i) {
            registered_[addrs[i]] = static_cast<size_t>(sizes[i]);
            totalSize += sizes[i];
        }
        UC_DEBUG("YuanRong device memory registered: buffers={}, total_size={}", addrs.size(),
                 totalSize);
        return Status::OK();
    }

    Expected<std::vector<bool>> LookupYuanRong(const Detail::BlockId* blocks, size_t num)
    {
        std::vector<std::string> keys;
        keys.reserve(num);
        for (size_t i = 0; i < num; ++i) { keys.push_back(MakeLookupKey(config_, blocks[i])); }

        std::vector<bool> exists;
        auto start = NowTime::Now();
        auto status = heteroClient_->Exist(keys, exists);
        auto end = NowTime::Now();
        if (status.IsError()) {
            return Status::Error("YuanRong Exist failed: " + status.ToString());
        }
        if (exists.size() != num) {
            return Status::Error("YuanRong Exist returned an unexpected result size");
        }
        const auto hitCount = std::count(exists.begin(), exists.end(), true);
        UC_DEBUG("YuanRong Lookup Exist keys={}, hit={}, cost={:.3f}ms, status={}.", num, hitCount,
                 (end - start) * 1e3, status.ToString());
        return exists;
    }

    static Config ParseConfig(const Detail::Dictionary& input)
    {
        Config config;
        input.Get("yuanrong_host", config.host);
        input.GetNumber("yuanrong_port", config.port);
        input.Get("yuanrong_namespace", config.nameSpace);
        if (config.nameSpace.empty()) { input.Get("unique_id", config.nameSpace); }
        input.Get("yuanrong_enable_remote_h2d", config.enableRemoteH2D);
        input.GetNumber("device_id", config.deviceId);
        input.GetNumbers("tensor_size_list", config.tensorSizes);
        input.GetNumber("shard_size", config.shardSize);
        input.GetNumber("block_size", config.blockSize);
        input.GetNumber("yuanrong_timeout_ms", config.timeoutMs);
        input.GetNumber("yuanrong_waiting_queue_depth", config.waitingQueueDepth);
        input.GetNumber("yuanrong_load_worker_count", config.loadWorkerCount);
        input.GetNumber("yuanrong_dump_prerequisite_worker_count",
                        config.dumpPrerequisiteWorkerCount);
        input.GetNumber("yuanrong_recovery_batch_size", config.recoveryBatchSize);
        input.GetNumber("yuanrong_host_buffer_count", config.hostBufferCount);
        config.hostBufferCountExplicit = config.hostBufferCount != 0;
        input.GetNumber("yuanrong_host_buffer_capacity_gb", config.hostBufferCapacityGb);
        input.GetNumber("yuanrong_h2d_stream_count", config.h2dStreamCount);
        input.GetNumber("yuanrong_backfill_worker_count", config.backfillWorkerCount);
        input.GetNumber("yuanrong_backfill_queue_depth", config.backfillQueueDepth);
        input.GetNumber("yuanrong_posix_max_inflight_gb", config.posixMaxInflightGb);
        input.Get("cpu_affinity_cores", config.cpuAffinityCores);
        input.Get("io_direct", config.ioDirect);
        input.Get("posix_io_engine", config.posixIoEngine);
        input.Get("store_backend", config.storeBackend);
        input.GetNumbers("gpu_kv_buffer_addrs", config.gpuKvBufferAddrs);
        input.GetNumbers("gpu_kv_buffer_sizes", config.gpuKvBufferSizes);
        config.memoryAlignment =
            config.ioDirect ? kDirectIoMemoryAlignment : kDefaultMemoryAlignment;
        config.objectSize =
            std::accumulate(config.tensorSizes.begin(), config.tensorSizes.end(), size_t{0});
        DeriveHostBufferCount(config);
        DerivePosixPersistence(config);
        return config;
    }

    static void DeriveHostBufferCount(Config& config)
    {
        if (config.deviceId < 0 || config.storeBackend == nullptr) {
            config.hostBufferCount = 0;
            return;
        }
        if (config.hostBufferCountExplicit || config.objectSize == 0 ||
            config.hostBufferCapacityGb == 0 ||
            config.hostBufferCapacityGb > (std::numeric_limits<uint64_t>::max() >> 30)) {
            return;
        }
        const auto capacityBytes = static_cast<uint64_t>(config.hostBufferCapacityGb) << 30;
        config.hostBufferCount = DeriveYuanRongHostBufferCount(
            config.objectSize, config.recoveryBatchSize, config.loadWorkerCount,
            config.backfillWorkerCount, capacityBytes);
    }

    static void DerivePosixPersistence(Config& config)
    {
        config.posixDumpBatchSize = 0;
        if (config.storeBackend == nullptr || config.objectSize == 0 ||
            config.posixMaxInflightGb == 0 ||
            config.posixMaxInflightGb > (std::numeric_limits<uint64_t>::max() >> 30)) {
            return;
        }
        const auto maxInflightBytes = static_cast<uint64_t>(config.posixMaxInflightGb) << 30;
        config.posixDumpBatchSize =
            DeriveYuanRongPosixDumpBatchSize(config.objectSize, maxInflightBytes);
    }

    static Status CheckConfig(const Config& config)
    {
        if (config.host.empty()) { return Status::InvalidParam("yuanrong_host is required"); }
        if (config.port <= 0 || config.port > 65535) {
            return Status::InvalidParam("invalid yuanrong_port({})", config.port);
        }
        if (config.nameSpace.empty()) {
            return Status::InvalidParam("yuanrong_namespace or unique_id is required");
        }
        auto validChar = [](unsigned char ch) {
            return std::isalnum(ch) != 0 || ch == '-' || ch == '_' || ch == '.';
        };
        if (!std::all_of(config.nameSpace.begin(), config.nameSpace.end(), validChar)) {
            return Status::InvalidParam("yuanrong_namespace contains unsupported characters");
        }
        if (config.timeoutMs > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            return Status::InvalidParam("yuanrong_timeout_ms is too large");
        }
        if (config.waitingQueueDepth <= 1) {
            return Status::InvalidParam("yuanrong_waiting_queue_depth({}) must be greater than 1",
                                        config.waitingQueueDepth);
        }
        if (config.loadWorkerCount == 0) {
            return Status::InvalidParam("yuanrong_load_worker_count must be greater than 0");
        }
        if (config.dumpPrerequisiteWorkerCount == 0) {
            return Status::InvalidParam(
                "yuanrong_dump_prerequisite_worker_count must be greater than 0");
        }
        if (config.h2dStreamCount == 0) {
            return Status::InvalidParam("yuanrong_h2d_stream_count must be greater than 0");
        }
        if (config.storeBackend != nullptr &&
            (config.posixMaxInflightGb == 0 ||
             config.posixMaxInflightGb > (std::numeric_limits<size_t>::max() >> 30))) {
            return Status::InvalidParam("invalid yuanrong_posix_max_inflight_gb({})",
                                        config.posixMaxInflightGb);
        }
        if (config.storeBackend != nullptr && config.posixIoEngine != "psync" &&
            config.posixIoEngine != "aio") {
            return Status::InvalidParam("invalid posix_io_engine({}) for YuanRong|Posix",
                                        config.posixIoEngine);
        }
        if (config.storeBackend != nullptr && config.posixIoEngine == "aio" && !config.ioDirect) {
            return Status::InvalidParam(
                "YuanRong|Posix posix_io_engine=aio requires io_direct=true");
        }
        if (config.deviceId < 0) { return Status::OK(); }
        if (config.gpuKvBufferAddrs.size() != config.gpuKvBufferSizes.size()) {
            return Status::InvalidParam(
                "gpu_kv_buffer_addrs({}) and gpu_kv_buffer_sizes({}) must have the same size",
                config.gpuKvBufferAddrs.size(), config.gpuKvBufferSizes.size());
        }
        for (size_t i = 0; i < config.gpuKvBufferAddrs.size(); ++i) {
            if (config.gpuKvBufferAddrs[i] == 0 || config.gpuKvBufferSizes[i] == 0) {
                return Status::InvalidParam("invalid GPU KV buffer at index({})", i);
            }
        }
        if (config.tensorSizes.empty() || config.objectSize == 0) {
            return Status::InvalidParam("tensor_size_list is required in worker mode");
        }
        if (config.shardSize == 0 || config.blockSize == 0 ||
            config.blockSize % config.shardSize != 0) {
            return Status::InvalidParam("invalid shard/block size");
        }
        if (config.storeBackend != nullptr) {
            if (config.posixDumpBatchSize == 0) {
                return Status::InvalidParam(
                    "YuanRong object requires {} bytes, exceeding "
                    "yuanrong_posix_max_inflight_gb({}GB)",
                    config.objectSize, config.posixMaxInflightGb);
            }
            if (config.recoveryBatchSize == 0) {
                return Status::InvalidParam("yuanrong_recovery_batch_size must be greater than 0");
            }
            if (config.backfillWorkerCount == 0) {
                return Status::InvalidParam(
                    "yuanrong_backfill_worker_count must be greater than 0");
            }
            if (config.backfillQueueDepth == 0) {
                return Status::InvalidParam("yuanrong_backfill_queue_depth must be greater than 0");
            }
            if (!config.hostBufferCountExplicit &&
                (config.hostBufferCapacityGb == 0 ||
                 config.hostBufferCapacityGb > (std::numeric_limits<uint64_t>::max() >> 30))) {
                return Status::InvalidParam("invalid yuanrong_host_buffer_capacity_gb({})",
                                            config.hostBufferCapacityGb);
            }
            if (config.hostBufferCount < config.recoveryBatchSize) {
                if (config.hostBufferCountExplicit) {
                    return Status::InvalidParam(
                        "yuanrong_host_buffer_count({}) must be greater than or equal to "
                        "yuanrong_recovery_batch_size({})",
                        config.hostBufferCount, config.recoveryBatchSize);
                }
                return Status::InvalidParam(
                    "YuanRong host buffer capacity({}GB) can provide {} buffers, fewer than "
                    "yuanrong_recovery_batch_size({}); increase "
                    "yuanrong_host_buffer_capacity_gb, reduce the batch size, or set "
                    "yuanrong_host_buffer_count explicitly",
                    config.hostBufferCapacityGb, config.hostBufferCount, config.recoveryBatchSize);
            }
            if (config.hostBufferCount >= std::numeric_limits<uint32_t>::max()) {
                return Status::InvalidParam("yuanrong_host_buffer_count({}) must be less than {}",
                                            config.hostBufferCount,
                                            std::numeric_limits<uint32_t>::max());
            }
        }
        if (config.ioDirect && config.storeBackend != nullptr) {
            if (config.objectSize % kDirectIoMemoryAlignment != 0) {
                return Status::InvalidParam(
                    "YuanRong object size must be aligned to 4096 bytes for io_direct");
            }
        }
        return Status::OK();
    }

    static std::string EscapeJsonString(std::string_view value)
    {
        constexpr char hex[] = "0123456789abcdef";
        std::string escaped;
        escaped.reserve(value.size());
        for (const auto ch : value) {
            const auto byte = static_cast<unsigned char>(ch);
            switch (ch) {
                case '\"': escaped += "\\\""; break;
                case '\\': escaped += "\\\\"; break;
                case '\b': escaped += "\\b"; break;
                case '\f': escaped += "\\f"; break;
                case '\n': escaped += "\\n"; break;
                case '\r': escaped += "\\r"; break;
                case '\t': escaped += "\\t"; break;
                default:
                    if (byte < 0x20) {
                        escaped += "\\u00";
                        escaped += hex[byte >> 4];
                        escaped += hex[byte & 0x0f];
                    } else {
                        escaped += ch;
                    }
            }
        }
        return escaped;
    }

    static void ShowConfig(const Config& config)
    {
        constexpr double bytesPerGib = 1024.0 * 1024.0 * 1024.0;
        const auto hostBufferPoolBytes = config.objectSize * config.hostBufferCount;
        const auto hostBufferCountSource =
            config.deviceId < 0 || config.storeBackend == nullptr
                ? "disabled"
                : (config.hostBufferCountExplicit ? "explicit" : "derived");
        const auto posixDumpBatchBytes = config.objectSize * config.posixDumpBatchSize;
        const auto maxInflightBytes =
            config.posixMaxInflightGb <= (std::numeric_limits<size_t>::max() >> 30)
                ? config.posixMaxInflightGb << 30
                : 0;
        const auto maxInflightBatches =
            posixDumpBatchBytes == 0 ? 0 : maxInflightBytes / posixDumpBatchBytes;
        const auto storeBackend =
            config.storeBackend ? config.storeBackend->Readme() : std::string{"none"};
        UC_INFO(
            "{{\"store\":\"YuanRongStore\",\"host\":\"{}\",\"port\":{},"
            "\"namespace\":\"{}\",\"enable_remote_h2d\":{},"
            "\"device_memory_pre_registration\":{},\"device_id\":{},"
            "\"object_size_bytes\":{},\"memory_alignment\":{},\"io_direct\":{},"
            "\"posix_io_engine\":\"{}\",\"timeout_ms\":{},\"load_worker_count\":{},"
            "\"dump_prerequisite_worker_count\":{},\"recovery_batch_size\":{},"
            "\"host_buffer_count\":{},\"host_buffer_count_source\":\"{}\","
            "\"host_buffer_capacity_gb\":{},\"host_buffer_pool_size_bytes\":{},"
            "\"host_buffer_pool_size_gib\":{:.3f},\"h2d_stream_count\":{},"
            "\"backfill_worker_count\":{},\"backfill_queue_depth\":{},"
            "\"persistence_queue_depth\":{},\"posix_dump_batch_size\":{},"
            "\"posix_dump_batch_bytes\":{},\"posix_max_inflight_gb\":{},"
            "\"posix_max_inflight_batches\":{},\"store_backend\":\"{}\"}}",
            EscapeJsonString(config.host), config.port, EscapeJsonString(config.nameSpace),
            config.enableRemoteH2D, config.enableDeviceMemoryPreRegistration, config.deviceId,
            config.objectSize, config.memoryAlignment, config.ioDirect,
            EscapeJsonString(config.posixIoEngine), config.timeoutMs, config.loadWorkerCount,
            config.dumpPrerequisiteWorkerCount, config.recoveryBatchSize, config.hostBufferCount,
            hostBufferCountSource, config.hostBufferCapacityGb, hostBufferPoolBytes,
            static_cast<double>(hostBufferPoolBytes) / bytesPerGib, config.h2dStreamCount,
            config.backfillWorkerCount, config.backfillQueueDepth, kPersistenceQueueDepth,
            config.posixDumpBatchSize, posixDumpBatchBytes, config.posixMaxInflightGb,
            maxInflightBatches, EscapeJsonString(storeBackend));
    }
};

}  // namespace UC::YuanRongStore

extern "C" UC::StoreV1* MakeYuanRongStore() { return new UC::YuanRongStore::YuanRongStore(); }
