import pytest

from dbt.cli.main import dbtRunner
from tests.functional.minimal_cli.fixtures import BaseConfigProject


class TestMinimalCli(BaseConfigProject):
    """Test the minimal/happy-path for the CLI using the dbtRunner"""

    @pytest.fixture(scope="class")
    def runner(self):
        return dbtRunner()

    def test_invoke_kwargs(self, runner, project):
        runner.invoke(["deps"])
        result = runner.invoke(
            ["run"],
            log_format="some_log_format",
            log_path="some_log_path",
            version_check=False,
            profiles_dir="some_profile_dir",
            profile_name="some_profile_name",
            target_dir="some_target_dir",
        )
        assert result[0].args["log_format"] == "some_log_format"
        assert result[0].args["log_path"] == "some_log_path"
        # assert not result[0].args['version_check']
        assert result[0].args["project_dir"] == "some_project_dir"
        assert result[0].args["profiles_dir"] == "some_profile_dir"
        assert result[0].args["profile_name"] == "some_profile_name"
        assert result[0].args["target_dir"] == "some_target_dir"
