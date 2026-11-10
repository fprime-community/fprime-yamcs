"""
Unit tests for the YAMCS-backed GDS transport (fprime_yamcs.gds.transport)
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import logging

import pytest

import yamcs.client  # noqa: F401  hard dependency: a broken yamcs-client must fail these tests, not skip them

from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.numerical_types import F64Type, I32Type
from fprime_gds.common.models.serialize.string_type import StringType
from fprime_gds.common.transport import TransportationException
from fprime_yamcs.gds.transport import (
    redact_url,
    split_credentials,
    TRANSFER_STATE_COMPLETED,
    TRANSFER_STATE_FAILED,
    YamcsClient,
    YamcsWrapper,
)

StringArgType = StringType.construct_type("StringArgType", max_length=40)


class TestNameMapping:
    def make_wrapper(self, namespace="/MyDeployment"):
        wrapper = YamcsWrapper()
        wrapper.namespace = namespace
        return wrapper

    def test_to_yamcs_param_name(self):
        wrapper = self.make_wrapper()
        assert (
            wrapper.to_yamcs_param_name("Ref.blockDrv.BD_Cycles")
            == "/MyDeployment/Ref/blockDrv/BD_Cycles"
        )

    def test_to_yamcs_cmd_name_matches_param_name(self):
        wrapper = self.make_wrapper()
        name = "Ref.cmdDisp.CMD_NO_OP"
        assert wrapper.to_yamcs_cmd_name(name) == wrapper.to_yamcs_param_name(name)

    def test_to_fprime_name_strips_namespace(self):
        wrapper = self.make_wrapper()
        assert (
            wrapper.to_fprime_name("/MyDeployment/Ref/blockDrv/BD_Cycles")
            == "Ref.blockDrv.BD_Cycles"
        )

    def test_to_fprime_name_no_namespace(self):
        wrapper = self.make_wrapper(namespace="")
        assert wrapper.to_fprime_name("/Ref/blockDrv/BD_Cycles") == "Ref.blockDrv.BD_Cycles"

    def test_roundtrip(self):
        wrapper = self.make_wrapper()
        original = "Ref.sendBuffComp.SendState"
        assert wrapper.to_fprime_name(wrapper.to_yamcs_param_name(original)) == original

    def test_to_yamcs_qualified_arg(self):
        wrapper = self.make_wrapper()
        assert (
            wrapper.to_yamcs_qualified_arg("/MyDeployment/Ref/cmdDisp/CMD_NO_OP_STRING", "arg1")
            == "Ref/cmdDisp/CMD_NO_OP_STRING|arg1"
        )

    def test_disconnect_resets_state(self):
        wrapper = self.make_wrapper()
        wrapper.instance = "instance"
        wrapper.disconnect()
        assert wrapper.namespace == ""
        assert wrapper.instance is None
        assert wrapper.yamcs_client is None
        assert wrapper.subscriptions == []


class TestParseUri:
    def test_plain_host_port(self):
        assert YamcsClient._parse_uri("yamcs://localhost:8090") == "http://localhost:8090"

    def test_no_scheme(self):
        assert YamcsClient._parse_uri("localhost:8090") == "http://localhost:8090"

    def test_secure_scheme(self):
        assert YamcsClient._parse_uri("yamcs+https://yamcs.example.com:443") == (
            "https://yamcs.example.com:443"
        )

    def test_trailing_path_stripped(self):
        assert YamcsClient._parse_uri("yamcs://host:8090/some/path") == "http://host:8090"


class TestBuildValueObject:
    def test_bool_from_string(self):
        obj = YamcsClient._build_value_object("True", BoolType)
        assert obj.val is True
        obj = YamcsClient._build_value_object("false", BoolType)
        assert obj.val is False

    def test_bool_from_number(self):
        assert YamcsClient._build_value_object(1, BoolType).val is True
        assert YamcsClient._build_value_object(0, BoolType).val is False

    def test_integer_from_string(self):
        assert YamcsClient._build_value_object("42", I32Type).val == 42

    def test_float_from_string(self):
        assert YamcsClient._build_value_object("3.5", F64Type).val == 3.5

    def test_string_passthrough(self):
        assert YamcsClient._build_value_object("hello", StringArgType).val == "hello"


class TestBuildEventDataMissingArgs:
    def make_client(self, arg_specs, extra):
        client = YamcsClient.__new__(YamcsClient)
        client.yamcs = YamcsWrapper()
        client._event_by_leaf = {}
        template = MagicMock()
        template.get_args.return_value = arg_specs
        client.dictionaries = MagicMock()
        client.dictionaries.event_name = {"Ref.event": template}
        event = MagicMock()
        event.event_type = "Ref.event"
        event.extra = extra
        event.generation_time = None
        return client, event

    def test_missing_numeric_arg_defaults_to_zero(self):
        client, event = self.make_client([("count", "", I32Type)], {})
        event_data = client._build_event_data(event)
        assert event_data.args[0].val == 0

    def test_missing_string_arg_defaults_to_empty(self):
        client, event = self.make_client([("name", "", StringArgType)], {})
        event_data = client._build_event_data(event)
        assert event_data.args[0].val == ""

    def test_missing_bool_arg_defaults_to_false(self):
        client, event = self.make_client([("flag", "", BoolType)], {})
        event_data = client._build_event_data(event)
        assert event_data.args[0].val is False


def make_transfer_service(states):
    """Build a mock file transfer service whose subscription yields the given states"""
    service = MagicMock()
    subscription = MagicMock()
    transfers = []
    for state in states:
        current = MagicMock()
        current.state = state
        current.failure_reason = "simulated failure"
        transfers.append(current)
    subscription.get_transfer.side_effect = transfers
    service.create_transfer_subscription.return_value = subscription
    return service, subscription


class TestAwaitTransfer:
    def test_completed_transfer_returned(self):
        service, subscription = make_transfer_service([TRANSFER_STATE_COMPLETED])
        transfer = MagicMock()
        transfer.id = "transfer-1"
        result = YamcsClient._await_transfer(service, transfer, timeout=5)
        assert str(result.state) == TRANSFER_STATE_COMPLETED
        subscription.cancel.assert_called_once()

    def test_failed_transfer_raises(self):
        service, subscription = make_transfer_service([TRANSFER_STATE_FAILED])
        transfer = MagicMock()
        transfer.id = "transfer-2"
        with pytest.raises(TransportationException, match="failed"):
            YamcsClient._await_transfer(service, transfer, timeout=5)
        subscription.cancel.assert_called_once()

    def test_timeout_raises(self):
        service, subscription = make_transfer_service([])
        transfer = MagicMock()
        transfer.id = "transfer-3"
        with pytest.raises(TransportationException, match="timed out"):
            YamcsClient._await_transfer(service, transfer, timeout=0)
        subscription.cancel.assert_called_once()


class TestDiscoverInstanceAndProcessor:
    def make_lib_client(self, mocker, instances, processors):
        lib_client = MagicMock()
        lib_client.list_instances.return_value = instances
        lib_client.list_processors.return_value = processors
        mocker.patch(
            "fprime_yamcs.gds.transport.YamcsLibClient", return_value=lib_client
        )
        return lib_client

    @staticmethod
    def named(name, state=None):
        obj = MagicMock()
        obj.name = name
        if state is not None:
            obj.state = state
        return obj

    def test_no_running_instances_raises(self, mocker):
        self.make_lib_client(mocker, [self.named("a", "STOPPED")], [])
        with pytest.raises(RuntimeError, match="No running YAMCS instances"):
            YamcsClient._discover_instance_and_processor("http://localhost:8090")

    def test_multiple_running_instances_raises(self, mocker):
        instances = [self.named("a", "RUNNING"), self.named("b", "RUNNING")]
        self.make_lib_client(mocker, instances, [])
        with pytest.raises(RuntimeError, match="Multiple running YAMCS instances"):
            YamcsClient._discover_instance_and_processor("http://localhost:8090")

    def test_no_processors_raises(self, mocker):
        self.make_lib_client(mocker, [self.named("a", "RUNNING")], [])
        with pytest.raises(RuntimeError, match="No processors found"):
            YamcsClient._discover_instance_and_processor("http://localhost:8090")

    def test_prefers_realtime_processor(self, mocker):
        processors = [self.named("replay"), self.named("realtime")]
        self.make_lib_client(mocker, [self.named("a", "RUNNING")], processors)
        instance, processor = YamcsClient._discover_instance_and_processor(
            "http://localhost:8090"
        )
        assert instance == "a"
        assert processor == "realtime"

    def test_falls_back_to_first_processor(self, mocker):
        processors = [self.named("replay")]
        self.make_lib_client(mocker, [self.named("a", "RUNNING")], processors)
        _, processor = YamcsClient._discover_instance_and_processor("http://localhost:8090")
        assert processor == "replay"


class TestPluginRegistration:
    def test_entry_point_registers_yamcs_transport(self):
        from fprime_gds.plugin.system import Plugins

        if "data_transport" not in Plugins.get_all_categories():
            pytest.skip("installed fprime-gds predates the data_transport plugin category")
        yamcs_plugins = [
            plugin.plugin_class
            for plugin in Plugins(["data_transport"]).get_plugins("data_transport")
            if plugin.get_name() == "yamcs"
        ]
        assert yamcs_plugins == [YamcsClient], "exactly one yamcs plugin, this package's class, must be registered"

    def test_instance_and_processor_overrides_are_validated_by_discovery(self):
        client = YamcsClient(yamcs_url="http://host:8090", yamcs_instance="sim", yamcs_processor="replay")
        client.yamcs = MagicMock()
        with patch.object(YamcsClient, "_discover_instance_and_processor", return_value=("sim", "replay")) as discover:
            client.connect()
        discover.assert_called_once_with("http://host:8090", "sim", "replay")
        client.yamcs.configure.assert_called_once_with("http://host:8090", "sim", "replay")

    def test_instance_override_discovers_processor(self):
        lib_client = MagicMock()
        lib_client.list_instances.return_value = [
            SimpleNamespace(state="RUNNING", name="sim"),
            SimpleNamespace(state="RUNNING", name="other"),
        ]
        lib_client.list_processors.return_value = [SimpleNamespace(name="replay"), SimpleNamespace(name="realtime")]
        with patch("fprime_yamcs.gds.transport.YamcsLibClient", return_value=lib_client):
            assert YamcsClient._discover_instance_and_processor("http://host:8090", "sim") == ("sim", "realtime")
            lib_client.list_processors.return_value = [SimpleNamespace(name="replay")]
            assert YamcsClient._discover_instance_and_processor("http://host:8090", "sim") == ("sim", "replay")
            with pytest.raises(RuntimeError, match="not running"):
                YamcsClient._discover_instance_and_processor("http://host:8090", "missing")
            with pytest.raises(RuntimeError, match="processor nope not found"):
                YamcsClient._discover_instance_and_processor("http://host:8090", "sim", "nope")

    def test_processor_without_instance_is_validated_against_the_discovered_instance(self):
        YamcsClient.check_arguments(yamcs_url="http://host:8090", yamcs_processor="replay")
        lib_client = MagicMock()
        lib_client.list_instances.return_value = [SimpleNamespace(name="only", state="RUNNING")]
        lib_client.list_processors.return_value = [SimpleNamespace(name="realtime"), SimpleNamespace(name="replay")]
        with patch("fprime_yamcs.gds.transport.YamcsLibClient", return_value=lib_client):
            assert YamcsClient._discover_instance_and_processor("http://host:8090", None, "replay") == ("only", "replay")
            with pytest.raises(RuntimeError, match="processor zzz not found in instance only"):
                YamcsClient._discover_instance_and_processor("http://host:8090", None, "zzz")
        lib_client.list_processors.assert_called_with(instance="only")

    @pytest.mark.parametrize(
        "url, uri, http_url",
        [
            ("http://host:8090", "yamcs://host:8090", "http://host:8090"),
            ("https://host", "yamcs+https://host", "https://host"),
            ("host:8090", "yamcs://host:8090", "http://host:8090"),
        ],
    )
    def test_connection_uri_mapping(self, url, uri, http_url):
        assert YamcsClient.get_connection_uri(yamcs_url=url, yamcs_instance="sim", yamcs_processor="p") == uri
        assert YamcsClient._parse_uri(uri) == http_url

    def test_rejects_non_http_url(self):
        with pytest.raises(ValueError, match="http:// or https://"):
            YamcsClient.check_arguments(yamcs_url="ftp://host:8090")
        with pytest.raises(ValueError):
            YamcsClient.get_connection_uri(yamcs_url="yamcs://host")

    def test_default_discovery_connect_configures_and_subscribes(self):
        client = YamcsClient(yamcs_url="http://host:8090")
        client.yamcs = MagicMock()
        discovered = ("auto", "realtime")
        with patch.object(YamcsClient, "_discover_instance_and_processor", return_value=discovered) as discover:
            with patch.object(YamcsClient, "_build_parameter_list", return_value=["/a", "/b"]):
                client.connect()
        discover.assert_called_once_with("http://host:8090", None, None)
        client.yamcs.configure.assert_called_once_with("http://host:8090", "auto", "realtime")
        kwargs = client.yamcs.subscribe.call_args.kwargs
        assert kwargs["parameter_names"] == ["/a", "/b"]
        assert kwargs["on_parameter_callback"] == client._on_parameter_data
        assert kwargs["on_event_callback"] == client._on_event_data

    def test_arguments_flow_through_gds_command_line(self):
        from fprime_gds.plugin.system import Plugins

        if "data_transport" not in Plugins.get_all_categories():
            pytest.skip("installed fprime-gds predates the data_transport plugin category")
        from fprime_gds.executables.cli import MiddleWareParser, ParserBase

        args, _ = ParserBase.parse_args(
            [MiddleWareParser],
            arguments=[
                "--data-transport-selection", "yamcs",
                "--yamcs-url", "https://yamcs.example:8443",
                "--yamcs-instance", "sim",
                "--yamcs-processor", "replay",
            ],
            client=True,
        )
        transport = args.connection_transport()
        assert type(transport) is YamcsClient
        assert transport.instance_override == "sim"
        assert transport.processor_override == "replay"
        assert args.connection_uri == "yamcs+https://yamcs.example:8443"
        reproduced = MiddleWareParser().reproduce_cli_args(args)
        for flag, value in [
            ("--data-transport-selection", "yamcs"),
            ("--yamcs-url", "https://yamcs.example:8443"),
            ("--yamcs-instance", "sim"),
            ("--yamcs-processor", "replay"),
        ]:
            assert reproduced[reproduced.index(flag) + 1] == value, (flag, reproduced)
        with pytest.raises(SystemExit):
            ParserBase.parse_args(
                [MiddleWareParser],
                arguments=["--data-transport-selection", "yamcs", "--yamcs-url", "ftp://host:8090"],
                client=True,
            )

    def test_entry_point_is_harmless_on_fprime_gds_without_category(self):
        from fprime_gds.plugin.system import Plugins

        if "data_transport" in Plugins.get_all_categories():
            pytest.skip("fprime-gds has the data_transport category; covered by the registration test")
        plugins = Plugins()
        assert "data_transport" not in plugins.get_categories()
        for category in plugins.get_categories():
            assert YamcsClient not in [plugin.plugin_class for plugin in plugins.get_plugins(category)]
        builtin_names = {plugin.get_name() for plugin in plugins.get_plugins("communication")}
        assert {"ip", "uart"} <= builtin_names, "the stock GDS plugins must survive the yamcs entry point"


class TestRedactUrl:
    def test_redacts_userinfo_only(self):
        assert redact_url("http://user:secret@host:8090/path") == "http://host:8090/path"
        assert redact_url("https://user@host") == "https://host"
        assert redact_url("http://host:8090") == "http://host:8090"
        assert redact_url("http://u:p@[::1]:8090") == "http://[::1]:8090"
        assert redact_url("http://u:p@host:port") == "http://host:port"

    def test_check_arguments_error_does_not_leak_credentials(self):
        with pytest.raises(ValueError) as info:
            YamcsClient.check_arguments(yamcs_url="ftp://user:secret@host:8090")
        assert "secret" not in str(info.value) and "ftp://host:8090" in str(info.value)

    def test_split_credentials(self):
        from yamcs.client import Credentials

        assert split_credentials("http://host:8090") == ("http://host:8090", None)
        url, credentials = split_credentials("https://user:secret@host:8443/base")
        assert url == "https://host:8443/base"
        assert isinstance(credentials, Credentials)
        assert (credentials.username, credentials.password) == ("user", "secret")

    def test_credentials_are_passed_to_yamcs_client_not_the_url(self, caplog):
        lib_client = MagicMock()
        lib_client.list_instances.return_value = [SimpleNamespace(name="a", state="RUNNING")]
        lib_client.list_processors.return_value = [SimpleNamespace(name="realtime")]
        wrapper = YamcsWrapper()
        with patch("fprime_yamcs.gds.transport.YamcsLibClient", return_value=lib_client) as lib:
            assert YamcsClient._discover_instance_and_processor("http://user:secret@host:8090") == ("a", "realtime")
            with caplog.at_level(logging.INFO, logger="transport"):
                wrapper.configure("http://user:secret@host:8090", "a", "realtime")
        for call in lib.call_args_list:
            assert call.args == ("http://host:8090",)
            assert (call.kwargs["credentials"].username, call.kwargs["credentials"].password) == ("user", "secret")
        assert "secret" not in caplog.text and "http://host:8090" in caplog.text

    def test_module_import_does_not_import_yamcs_client(self):
        import subprocess
        import sys

        code = "import sys, fprime_yamcs.gds.transport; assert 'yamcs.client' not in sys.modules, 'eager import'"
        subprocess.run([sys.executable, "-c", code], check=True)

    @pytest.mark.parametrize(
        "instances, processors, kwargs",
        [
            ([], [], {}),
            (["a", "b"], [], {}),
            (["a"], ["realtime"], {"instance": "zzz"}),
            (["a"], ["realtime"], {"instance": "a", "processor_name": "zzz"}),
            (["a"], [], {}),
        ],
    )
    def test_discovery_errors_do_not_leak_credentials(self, instances, processors, kwargs):
        lib_client = MagicMock()
        lib_client.list_instances.return_value = [SimpleNamespace(name=n, state="RUNNING") for n in instances]
        lib_client.list_processors.return_value = [SimpleNamespace(name=n) for n in processors]
        with patch("fprime_yamcs.gds.transport.YamcsLibClient", return_value=lib_client):
            with pytest.raises(RuntimeError) as info:
                YamcsClient._discover_instance_and_processor("http://user:secret@host:8090", **kwargs)
        assert "secret" not in str(info.value)
        assert "host:8090" in str(info.value)


class TestEntryPointMetadata:
    def test_wheel_metadata_declares_fprime_gds_entry_point(self):
        from importlib.metadata import entry_points

        matches = [ep for ep in entry_points(group="fprime_gds") if ep.name == "yamcs"]
        assert [ep.value for ep in matches] == ["fprime_yamcs.gds.transport:YamcsClient"]
        assert matches[0].load() is YamcsClient
