import pytest

from dbt.tests.util import run_dbt
from tests.functional.compile.fixtures import (
    my_ephemeral_model_sql,
    another_ephemeral_model_sql,
    my_other_model_sql,
)


def get_lines(model_name):
    from dbt.tests.util import read_file

    f = read_file("target", "compiled", "test", "models", model_name + ".sql")
    return [line for line in f.splitlines() if line]


def file_exists(model_name):
    from dbt.tests.util import file_exists

    return file_exists("target", "compiled", "test", "models", model_name + ".sql")


class TestBase:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_ephemeral_model.sql": my_ephemeral_model_sql,
            "another_ephemeral_model.sql": another_ephemeral_model_sql,
            "my_other_model.sql": my_other_model_sql,
        }


class TestEphemeralModels(TestBase):
    def test_ephemeral_models(self, project):
        run_dbt(["compile"])

        assert get_lines("my_ephemeral_model") == ["select 1 as fun"]
        assert get_lines("another_ephemeral_model") == [
            "with __dbt__cte__my_ephemeral_model as (",
            "select 1 as fun",
            ")select * from __dbt__cte__my_ephemeral_model",
        ]
        assert get_lines("my_other_model") == [
            "with __dbt__cte__my_ephemeral_model as (",
            "select 1 as fun",
            "),  __dbt__cte__another_ephemeral_model as (",
            "select * from __dbt__cte__my_ephemeral_model",
            ")select * from __dbt__cte__another_ephemeral_model",
            "union all",
            "select 2 as fun",
        ]


class TestFirstSelector(TestBase):
    def test_first_selector(self, project):
        run_dbt(["compile", "--select", "my_ephemeral_model"])
        assert file_exists("my_ephemeral_model")
        assert not file_exists("another_ephemeral_model")
        assert not file_exists("my_other_model")


class TestMiddleSelector(TestBase):
    def test_first_selector(self, project):
        run_dbt(["compile", "--select", "another_ephemeral_model"])
        assert file_exists("my_ephemeral_model")
        assert file_exists("another_ephemeral_model")
        assert not file_exists("my_other_model")


class TestLastSelector(TestBase):
    def test_first_selector(self, project):
        run_dbt(["compile", "--select", "my_other_model"])
        assert file_exists("my_ephemeral_model")
        assert file_exists("another_ephemeral_model")
        assert file_exists("my_other_model")
