/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
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
#include <atomic>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <vector>
#include "datasystem/hetero_client.h"
#include "yuanrong_config.h"
#include "logger/logger.h"
#include "trans_manager.h"
#include "ucmstore_v1.h"

namespace UC::YuanrongStore {

namespace {

constexpr char kHexTable[] = "0123456789abcdef";

std::string BlockIdToKey(const Detail::BlockId& block)
{
    std::string out;
    out.resize(block.size() * 2);
    for (size_t i = 0; i < block.size(); ++i) {
        auto b = static_cast<uint8_t>(block[i]);
        out[i * 2] = kHexTable[b >> 4];
        out[i * 2 + 1] = kHexTable[b & 0x0F];
    }
    return out;
}

}  // namespace

class YuanrongStore : public StoreV1 {
    TransManager transMgr_;
    Config config_;
    bool transEnable_{false};
    std::shared_ptr<datasystem::HeteroClient> lookupClient_;

    std::atomic<bool> closed_{false};

public:
    ~YuanrongStore() override { Close(); }

    void Close()
    {
        if (closed_.exchange(true, std::memory_order_acq_rel)) { return; }

        transMgr_.Close();
        if (lookupClient_) {
            auto rc = lookupClient_->ShutDown();
            if (rc.IsError()) {
                UC_WARN("Failed to shut down Yuanrong lookup client: {}", rc.ToString());
            }
            lookupClient_.reset();
        }
    }

    Status Setup(const Detail::Dictionary& inConfig) override
    {
        auto config = ParseConfig(inConfig);
        auto s = CheckConfig(config);
        if (s.Failure()) [[unlikely]] {
            UC_ERROR("Config check failed: {}.", s);
            return s;
        }
        config_ = config;
        transEnable_ = config.deviceId >= 0;

        if (transEnable_) {
            s = transMgr_.Setup(config);
            if (s.Failure()) [[unlikely]] { return s; }
        } else {
            s = SetupLookupClient(config);
            if (s.Failure()) [[unlikely]] { return s; }
        }
        ShowConfig(config);
        return Status::OK();
    }

    std::string Readme() const override { return "YuanrongStore"; }

    Expected<std::vector<uint8_t>> Lookup(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return std::vector<uint8_t>{}; }

        auto res = LookupOnPrefix(blocks, num);
        if (!res) [[unlikely]] { return res.Error(); }

        std::vector<uint8_t> results(num, 0);
        const auto index = res.Value();
        for (ssize_t i = 0; i <= index; ++i) { results[i] = 1; }
        return results;
    }

    Expected<ssize_t> LookupOnPrefix(const Detail::BlockId* blocks, size_t num) override
    {
        if (num == 0) { return static_cast<ssize_t>(-1); }

        std::vector<std::string> keys;
        keys.reserve(num);
        for (size_t i = 0; i < num; ++i) { keys.push_back(BlockIdToKey(blocks[i]) + "_0"); }

        auto exists = BatchIsExist(keys);
        if (!exists) { return exists.Error(); }
        const auto& values = exists.Value();

        ssize_t firstMiss = -1;
        for (size_t i = 0; i < num; ++i) {
            if (!values[i]) {
                firstMiss = static_cast<ssize_t>(i);
                break;
            }
        }

        if (firstMiss == -1) { return static_cast<ssize_t>(num) - 1; }

        if (config_.storeBackend) {
            auto backendRes =
                config_.storeBackend->LookupOnPrefix(blocks + firstMiss, num - firstMiss);
            if (backendRes) {
                ssize_t backendHit = backendRes.Value();
                if (backendHit >= 0) { return firstMiss + backendHit; }
            }
        }

        return firstMiss - 1;
    }

    void Prefetch(const Detail::BlockId* blocks, size_t num) override
    {
        (void)blocks;
        (void)num;
    }

    Expected<Detail::TaskHandle> Load(Detail::TaskDesc task) override
    {
        if (!transEnable_) { return Status::Error("transfer is not enabled (scheduler mode)"); }
        TransTask transTask;
        transTask.type = TaskType::LOAD;
        transTask.brief = task.brief;
        auto s = BuildShards(task, transTask);
        if (s.Failure()) [[unlikely]] { return s; }
        return transMgr_.Submit(std::move(transTask));
    }

    Expected<Detail::TaskHandle> Dump(Detail::TaskDesc task) override
    {
        if (!transEnable_) { return Status::Error("transfer is not enabled (scheduler mode)"); }
        TransTask transTask;
        transTask.type = TaskType::DUMP;
        transTask.brief = task.brief;
        transTask.prerequisiteHandle = task.prerequisiteHandle;
        auto s = BuildShards(task, transTask);
        if (s.Failure()) [[unlikely]] { return s; }
        return transMgr_.Submit(std::move(transTask));
    }

    Expected<bool> Check(Detail::TaskHandle taskId) override { return transMgr_.Check(taskId); }

    Status Wait(Detail::TaskHandle taskId) override { return transMgr_.Wait(taskId); }

private:
    Status SetupLookupClient(const Config& config)
    {
        datasystem::ConnectOptions options;
        options.host = config.host;
        options.port = config.port;
        options.enableRemoteH2D = config.enableRemoteH2D;
        options.requestTimeoutMs = static_cast<int32_t>(config.timeoutMs);
        lookupClient_ = std::make_shared<datasystem::HeteroClient>(options);
        auto rc = lookupClient_->Init();
        if (rc.IsError()) {
            UC_ERROR("Failed to initialize Yuanrong lookup client: {}", rc.ToString());
            lookupClient_.reset();
            return Status::Error(rc.ToString());
        }
        return Status::OK();
    }

    Expected<std::vector<bool>> BatchIsExist(const std::vector<std::string>& keys)
    {
        if (keys.empty()) { return std::vector<bool>{}; }
        auto client = transEnable_ ? transMgr_.GetClient() : lookupClient_;
        if (!client) { return Status::Error("Yuanrong HeteroClient is not initialized"); }
        std::vector<bool> exists;
        auto rc = client->Exist(keys, exists);
        if (rc.IsError()) { return Status::Error(rc.ToString()); }
        if (exists.size() != keys.size()) {
            return Status::Error("Yuanrong Exist returned unexpected result size");
        }
        return exists;
    }

    Status BuildShards(const Detail::TaskDesc& desc, TransTask& out)
    {
        out.shards.reserve(desc.size());
        for (const auto& shard : desc) {
            std::string key = BlockIdToKey(shard.owner) + "_" + std::to_string(shard.index);

            if (shard.addrs.size() != config_.tensorSizeList.size()) {
                return Status::InvalidParam(
                    "key({}) address count({}) does not match tensor_size_list({})", key,
                    shard.addrs.size(), config_.tensorSizeList.size());
            }

            std::vector<void*> addrs(shard.addrs.begin(), shard.addrs.end());
            std::vector<size_t> sizes(config_.tensorSizeList.begin(), config_.tensorSizeList.end());

            out.shards.push_back(TransShard{std::move(key), shard.owner, shard.index,
                                            std::move(addrs), std::move(sizes)});
        }
        return Status::OK();
    }

    Config ParseConfig(const Detail::Dictionary& inConfig)
    {
        Config config;
        inConfig.Get("yuanrong_host", config.host);
        inConfig.GetNumber("yuanrong_port", config.port);
        inConfig.Get("yuanrong_enable_remote_h2d", config.enableRemoteH2D);
        inConfig.GetNumber("device_id", config.deviceId);
        inConfig.GetNumbers("tensor_size_list", config.tensorSizeList);
        inConfig.GetNumber("dump_queue_depth", config.dumpQueueDepth);
        inConfig.GetNumber("load_queue_depth", config.loadQueueDepth);
        inConfig.GetNumber("timeout_ms", config.timeoutMs);
        inConfig.GetNumber("stream_number", config.streamNumber);
        inConfig.Get("cpu_affinity_cores", config.cpuAffinityCores);
        inConfig.Get("io_direct", config.ioDirect);
        inConfig.GetNumber("local_rank_size", config.localRankSize);
        inConfig.Get("unique_id", config.uniqueId);
        inConfig.Get("store_backend", config.storeBackend);
        DeriveShareBufferNumber(inConfig, config);
        DeriveHostBufPoolSize(inConfig, config);
        return config;
    }

    void DeriveShareBufferNumber(const Detail::Dictionary& inConfig, Config& config)
    {
        size_t shareBufferCapacityGb = 0;
        inConfig.GetNumber("share_buffer_capacity_gb", shareBufferCapacityGb);
        if (shareBufferCapacityGb != 0) {
            config.shareBufferCapacity = shareBufferCapacityGb << 30;
        }

        const uint64_t shareUnit = std::accumulate(config.tensorSizeList.begin(),
                                                   config.tensorSizeList.end(), uint64_t{0});
        if (shareUnit > 0) {
            config.shareBufferNumber = static_cast<size_t>(config.shareBufferCapacity / shareUnit);
        }
    }

    void DeriveHostBufPoolSize(const Detail::Dictionary& inConfig, Config& config)
    {
        constexpr uint32_t kHostBufPerStream = 256;
        constexpr uint64_t kHostBufMaxPinnedBytes = 8ULL << 30;
        constexpr uint32_t kHostBufMinCount = 64;

        const uint64_t hostUnit = std::accumulate(config.tensorSizeList.begin(),
                                                  config.tensorSizeList.end(), uint64_t{0});
        if (hostUnit > 0) {
            uint64_t derived = static_cast<uint64_t>(config.streamNumber) * kHostBufPerStream;
            uint64_t capByMem = kHostBufMaxPinnedBytes / hostUnit;
            if (capByMem == 0) { capByMem = 1; }
            derived = std::min<uint64_t>(derived, capByMem);
            if (derived < kHostBufMinCount) {
                derived = std::min<uint64_t>(kHostBufMinCount, capByMem);
            }
            config.hostBufPoolSize = static_cast<uint32_t>(derived);
        }

        uint32_t explicitPool = 0;
        inConfig.GetNumber("host_buf_pool_size", explicitPool);
        if (explicitPool != 0) { config.hostBufPoolSize = explicitPool; }
    }

    Status CheckConfig(const Config& config)
    {
        if (config.host.empty()) { return Status::InvalidParam("yuanrong_host is required"); }
        if (config.port <= 0 || config.port > 65535) {
            return Status::InvalidParam("invalid yuanrong_port({})", config.port);
        }
        if (config.deviceId >= 0 && config.tensorSizeList.empty()) {
            return Status::InvalidParam("tensor_size_list is required in worker mode");
        }
        if (config.timeoutMs > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            return Status::InvalidParam("invalid timeout_ms({})", config.timeoutMs);
        }
        if (config.streamNumber < 1 || config.streamNumber > 32) {
            return Status::InvalidParam("invalid stream_number({})", config.streamNumber);
        }
        if (config.localRankSize > 1 && config.storeBackend) {
            if (config.uniqueId.empty()) {
                return Status::InvalidParam("unique_id is required for multi-rank shared miss");
            }
            if (config.shareBufferNumber == 0) {
                return Status::InvalidParam("share buffer cannot hold one shard");
            }
        }
        return Status::OK();
    }

    void ShowConfig(const Config& config)
    {
        constexpr const char* ns = "YuanrongStore";
        std::string buildType = UCM_BUILD_TYPE;
        if (buildType.empty()) { buildType = "Release"; }
        UC_INFO("{}-{}({}).", ns, UCM_COMMIT_ID, buildType);
        UC_INFO("{}::Host = {}", ns, config.host);
        UC_INFO("{}::Port = {}", ns, config.port);
        UC_INFO("{}::EnableRemoteH2D = {}", ns, config.enableRemoteH2D);
        UC_INFO("{}::DeviceId = {}", ns, config.deviceId);
        UC_INFO("{}::DumpQueueDepth = {}", ns, config.dumpQueueDepth);
        UC_INFO("{}::LoadQueueDepth = {}", ns, config.loadQueueDepth);
        UC_INFO("{}::HostBufPoolSize = {}", ns, config.hostBufPoolSize);
        UC_INFO("{}::TimeoutMs = {}", ns, config.timeoutMs);
        UC_INFO("{}::StreamNumber = {}", ns, config.streamNumber);
        UC_INFO("{}::CpuAffinityCores = {}", ns, config.cpuAffinityCores);
        UC_INFO("{}::IoDirect = {}", ns, config.ioDirect);
        UC_INFO("{}::LocalRankSize = {}", ns, config.localRankSize);
        UC_INFO("{}::UniqueId = {}", ns, config.uniqueId);
        UC_INFO("{}::ShareBufferCapacity = {}GB", ns, config.shareBufferCapacity >> 30);
        UC_INFO("{}::ShareBufferNumber = {}", ns, config.shareBufferNumber);
        UC_INFO("{}::StoreBackend = {}", ns, config.storeBackend ? "yes" : "none");
        UC_INFO("{}::TransEnable = {}", ns, transEnable_);
    }
};

}  // namespace UC::YuanrongStore

extern "C" UC::StoreV1* MakeYuanrongStore() { return new UC::YuanrongStore::YuanrongStore(); }
extern "C" void DestroyYuanrongStore(UC::StoreV1* p) { delete p; }
