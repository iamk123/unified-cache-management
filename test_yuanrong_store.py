import sys
import types
import unittest


class FakeObjectProxy:
    def __init__(self, wrapped):
        self.__wrapped__ = wrapped


class FakeLevel:
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class FakeFuture:
    def __init__(self, failed_keys=None):
        self.failed_keys = failed_keys or []
        self.calls = []

    def get(self, timeout_ms=60000):
        self.calls.append(timeout_ms)
        return self.failed_keys


class FakeBlob:
    def __init__(self, dev_ptr, size):
        self.dev_ptr = dev_ptr
        self.size = size


class FakeDeviceBlobList:
    def __init__(self, dev_idx, blob_list, src_offset=0):
        self.dev_idx = dev_idx
        self.blob_list = blob_list
        self.src_offset = src_offset


class FakeHeteroClient:
    instances = []

    def __init__(self, *args):
        self.args = args
        self.inited = False
        self.exists = []
        self.mset_calls = []
        self.mget_calls = []
        self.next_future = FakeFuture()
        FakeHeteroClient.instances.append(self)

    def init(self):
        self.inited = True

    def exist(self, keys):
        self.last_exist_keys = keys
        return self.exists

    def async_mset_d2h(self, keys, data_blob_list, set_param):
        self.mset_calls.append((keys, data_blob_list, set_param))
        return self.next_future

    def async_mget_h2d(self, keys, data_blob_list, sub_timeout_ms):
        self.mget_calls.append((keys, data_blob_list, sub_timeout_ms))
        return self.next_future


class FakeSetParam:
    pass


class UcmYuanrongStoreTest(unittest.TestCase):
    def setUp(self):
        FakeHeteroClient.instances.clear()
        self._saved_modules = {
            name: sys.modules.get(name)
            for name in (
                "wrapt",
                "numpy",
                "torch",
                "ucm.shared.infra.ucmlogger",
                "yr",
                "yr.datasystem",
                "yr.datasystem.hetero_client",
                "yr.datasystem.kv_client",
            )
        }
        wrapt_module = types.ModuleType("wrapt")
        wrapt_module.ObjectProxy = FakeObjectProxy
        numpy_module = types.ModuleType("numpy")
        numpy_module.ndarray = type("ndarray", (), {})
        torch_module = types.ModuleType("torch")
        torch_module.Tensor = type("Tensor", (), {})
        ucmlogger_module = types.ModuleType("ucm.shared.infra.ucmlogger")
        ucmlogger_module.Level = FakeLevel
        ucmlogger_module.setup = lambda *args, **kwargs: None
        ucmlogger_module.flush = lambda *args, **kwargs: None
        ucmlogger_module.isEnabledFor = lambda *args, **kwargs: True
        ucmlogger_module.log = lambda *args, **kwargs: None
        ucmlogger_module.log_rate_limit = lambda *args, **kwargs: None
        yr_module = types.ModuleType("yr")
        datasystem_module = types.ModuleType("yr.datasystem")
        hetero_client_module = types.ModuleType("yr.datasystem.hetero_client")
        kv_client_module = types.ModuleType("yr.datasystem.kv_client")
        hetero_client_module.HeteroClient = FakeHeteroClient
        hetero_client_module.Blob = FakeBlob
        hetero_client_module.DeviceBlobList = FakeDeviceBlobList
        kv_client_module.SetParam = FakeSetParam

        sys.modules["wrapt"] = wrapt_module
        sys.modules["numpy"] = numpy_module
        sys.modules["torch"] = torch_module
        sys.modules["ucm.shared.infra.ucmlogger"] = ucmlogger_module
        sys.modules["yr"] = yr_module
        sys.modules["yr.datasystem"] = datasystem_module
        sys.modules["yr.datasystem.hetero_client"] = hetero_client_module
        sys.modules["yr.datasystem.kv_client"] = kv_client_module

    def tearDown(self):
        for name, module in self._saved_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def make_store(self, config=None):
        from ucm.store.yuanrongstore.yuanrong_connector import UcmYuanrongStore

        base_config = {
            "host": "127.0.0.1",
            "port": 18482,
            "device_id": 2,
            "tensor_size": 64,
            "timeout_ms": 1234,
        }
        if config:
            base_config.update(config)
        return UcmYuanrongStore(base_config)

    def test_initializes_hetero_client_and_registers_factory(self):
        from ucm.store.factory_v1 import UcmConnectorFactoryV1
        from ucm.store.yuanrongstore.yuanrong_connector import UcmYuanrongStore

        store = UcmConnectorFactoryV1.create_connector(
            "UcmYuanrongStore",
            {
                "host": "127.0.0.1",
                "port": 18482,
                "device_id": 1,
                "tensor_size": 32,
            },
        )

        self.assertIsInstance(store, UcmYuanrongStore)
        client = FakeHeteroClient.instances[-1]
        self.assertTrue(client.inited)
        self.assertEqual(client.args[:2], ("127.0.0.1", 18482))

    def test_lookup_and_lookup_on_prefix_encode_keys_with_default_shard(self):
        store = self.make_store({"key_prefix": "prefix"})
        client = FakeHeteroClient.instances[-1]
        block_ids = [b"\x01" * 16, b"\x02" * 16, b"\x03" * 16]
        client.exists = [True, True, False]

        self.assertEqual(store.lookup(block_ids), [True, True, False])
        self.assertEqual(
            client.last_exist_keys,
            [
                "prefix:" + block_ids[0].hex() + ":0",
                "prefix:" + block_ids[1].hex() + ":0",
                "prefix:" + block_ids[2].hex() + ":0",
            ],
        )
        self.assertEqual(store.lookup_on_prefix(block_ids), 1)

    def test_dump_data_builds_device_blob_lists_and_waits_for_future(self):
        store = self.make_store()
        client = FakeHeteroClient.instances[-1]
        block_ids = [b"\x0a" * 16, b"\x0b" * 16]

        task = store.dump_data(block_ids, [3, 4], [[101, 102], [201, 202]])

        keys, blob_lists, set_param = client.mset_calls[-1]
        self.assertEqual(
            keys,
            [
                "ucm:" + block_ids[0].hex() + ":3",
                "ucm:" + block_ids[1].hex() + ":4",
            ],
        )
        self.assertIsNotNone(set_param)
        self.assertIs(task.future, client.next_future)
        self.assertEqual(task.keys, keys)
        self.assertEqual([blob.dev_ptr for blob in blob_lists[0].blob_list], [101, 102])
        self.assertEqual([blob.size for blob in blob_lists[0].blob_list], [64, 64])
        self.assertEqual(blob_lists[0].dev_idx, 2)

        store.wait(task)
        self.assertEqual(client.next_future.calls, [1234])

    def test_load_data_uses_async_mget_h2d_and_configured_timeout(self):
        store = self.make_store({"timeout_ms": 5678})
        client = FakeHeteroClient.instances[-1]
        block_id = b"\x0c" * 16

        task = store.load_data([block_id], [5], [[301]])

        keys, blob_lists, timeout_ms = client.mget_calls[-1]
        self.assertEqual(keys, ["ucm:" + block_id.hex() + ":5"])
        self.assertEqual(timeout_ms, 5678)
        self.assertEqual(blob_lists[0].blob_list[0].dev_ptr, 301)
        self.assertEqual(blob_lists[0].blob_list[0].size, 64)
        self.assertEqual(task.keys, keys)

    def test_wait_raises_when_future_returns_failed_keys(self):
        store = self.make_store()
        client = FakeHeteroClient.instances[-1]
        client.next_future = FakeFuture(["bad-key"])
        task = store.load_data([b"\x0d" * 16], [0], [[401]])

        with self.assertRaisesRegex(RuntimeError, "Transfer failed for 1 keys"):
            store.wait(task)

    def test_config_and_input_validation(self):
        from ucm.store.yuanrongstore.yuanrong_connector import UcmYuanrongStore

        with self.assertRaisesRegex(ValueError, "host"):
            UcmYuanrongStore({"port": 18482, "device_id": 0})
        with self.assertRaisesRegex(ValueError, "port"):
            UcmYuanrongStore({"host": "127.0.0.1", "device_id": 0})

        lookup_only_store = UcmYuanrongStore({"host": "127.0.0.1", "port": 18482})
        lookup_only_client = FakeHeteroClient.instances[-1]
        lookup_only_client.exists = [True]
        self.assertEqual(lookup_only_store.lookup([b"\x10" * 16]), [True])
        with self.assertRaisesRegex(ValueError, "transfer size"):
            lookup_only_store.dump_data([b"\x11" * 16], [0], [[601]])

        store = self.make_store()
        with self.assertRaisesRegex(ValueError, "same length"):
            store.dump_data([b"\x0e" * 16], [0, 1], [[501]])
        with self.assertRaisesRegex(ValueError, "address rows"):
            store.load_data([b"\x0f" * 16], [0], [])


if __name__ == "__main__":
    unittest.main()
