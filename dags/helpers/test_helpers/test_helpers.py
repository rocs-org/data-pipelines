import os
from .helpers import (
    check_if_var_exists_in_dag_conf,
    set_env_variable,
    set_env_variable_from_dag_config_if_present,
)


def test_check_if_var_exists_in_dag_conf():
    assert (
        check_if_var_exists_in_dag_conf("var", {"dag_run": {"conf": {"var": "value"}}})
        is True
    )
    assert (
        check_if_var_exists_in_dag_conf(
            "varrr", {"dag_run": {"conf": {"var": "value"}}}
        )
        is False
    )
    assert check_if_var_exists_in_dag_conf("var", {"dag_run": {}}) is False
    assert check_if_var_exists_in_dag_conf("var", {}) is False


def test_set_env_variable():
    set_env_variable("abc", "value")
    assert os.environ.get("abc") == "value"


def test_set_env_variable_from_dag_config():
    set_env_variable_from_dag_config_if_present("var")(
        {"dag_run": {"conf": {"var": "value"}}}
    )
    assert os.environ.get("var") == "value"
