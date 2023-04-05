import pytest

from dbt.cli.exceptions import DbtUsageException
from dbt.cli.main import dbtRunner
from unittest import mock


class TestDbtRunner:
    @pytest.fixture
    def dbt(self) -> dbtRunner:
        return dbtRunner()

    def test_group_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_command_invalid_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["deps", "--invalid-option"])
        assert type(res.exception) == DbtUsageException

    def test_command_mutually_exclusive_option(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["--warn-error", "--warn-error-options", '{"include": "all"}', "deps"])
        assert type(res.exception) == DbtUsageException

    def test_invalid_command(self, dbt: dbtRunner) -> None:
        res = dbt.invoke(["invalid-command"])
        assert type(res.exception) == DbtUsageException

    def test_invoke_version(self, dbt: dbtRunner) -> None:
        dbt.invoke(["--version"])

    def test_callbacks(self) -> None:
        mock_callback = mock.MagicMock()
        dbt = dbtRunner(callbacks=[mock_callback])
        # the `debug` command is one of the few commands wherein you don't need
        # to have a project to run it and it will emit events
        dbt.invoke(["debug"])
        mock_callback.assert_called()
