# pylint: disable=missing-function-docstring
"""Tests for the XCom checker and its helper functions."""
from unittest.mock import Mock

import astroid
import pytest
from pylint.testutils import CheckerTestCase, MessageTest

import pylint_airflow
from pylint_airflow.checkers.xcom import (
    PythonOperatorSpec,
    get_task_ids_to_python_callable_specs,
    get_xcoms_from_tasks,
)


class TestGetTaskIdsToPythonCallableSpecs:
    """Tests the get_task_ids_to_python_callable_specs helper function which detects the
    python_callable functions passed to PythonOperator constructions."""

    @pytest.mark.parametrize(
        "test_code",
        [
            """# empty module""",
            """
                print("test this")
                int("5")
            """,
            """
                x = 5
                y = x + 2
            """,
            """
                x = int(x="-5")  # keyword arg
                y = abs(x)  # positional arg
            """,
        ],
        ids=[
            "no code",
            "no assignments",
            "no calls",
            "no operators",
        ],
    )
    def test_should_return_empty_when_no_tasks(self, test_code):
        ast = astroid.parse(test_code)

        result = get_task_ids_to_python_callable_specs(ast)

        assert result == {}

    def test_should_skip_lambda_callable(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        lambda_task = PythonOperator(task_id="lambda_task", python_callable=lambda x: print(x))
        """
        ast = astroid.parse(test_code)

        result = get_task_ids_to_python_callable_specs(ast)

        assert result == {}

    def test_should_detect_builtin_callable(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        builtin_task = PythonOperator(task_id="builtin_task", python_callable=super)
        """
        ast = astroid.parse(test_code)

        result = get_task_ids_to_python_callable_specs(ast)

        expected_result = {
            "builtin_task": PythonOperatorSpec(ast.body[1].value, "super"),
        }

        assert result == expected_result

    @pytest.mark.xfail(reason="Test not yet written", raises=AssertionError, strict=True)
    def test_should_detect_imported_callable(self):
        assert False  # TODO: write this test

    def test_should_detect_local_function_callables(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        def task_func():
            print("bupkis")

        def aux_func():
            return 2 + 2

        local_task = PythonOperator(task_id="local_task", python_callable=task_func)
        another_task = PythonOperator(task_id="another_task", python_callable=aux_func)
        """
        ast = astroid.parse(test_code)

        result = get_task_ids_to_python_callable_specs(ast)

        expected_result = {
            "local_task": PythonOperatorSpec(ast.body[3].value, "task_func"),
            "another_task": PythonOperatorSpec(ast.body[4].value, "aux_func"),
        }

        assert result == expected_result


class TestGetXComsFromTasks:
    """Tests the get_xcoms_from_tasks helper function which detects the xcom pushes and pulls."""

    def test_should_return_empty_on_empty_input(self):
        result = get_xcoms_from_tasks(Mock(), {})
        # node argument isn't used when input dict is empty, so we can simply use a Mock object

        assert result == ({}, set())

    def test_should_skip_lambda_callable(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        lambda_task = PythonOperator(task_id="lambda_task", python_callable=lambda x: print(x))
        """
        ast = astroid.parse(test_code)
        test_task_ids_to_python_callable_specs = {
            "lambda_task": PythonOperatorSpec(ast.body[1].value, "<lambda>")
        }

        result = get_xcoms_from_tasks(ast, test_task_ids_to_python_callable_specs)

        assert result == ({}, set())

    def test_should_skip_builtin_callable(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        builtin_task = PythonOperator(task_id="builtin_task", python_callable=super)
        """
        ast = astroid.parse(test_code)

        test_task_ids_to_python_callable_specs = {
            "builtin_task": PythonOperatorSpec(ast.body[1].value, "super"),
        }

        result = get_xcoms_from_tasks(ast, test_task_ids_to_python_callable_specs)

        assert result == ({}, set())

    @pytest.mark.xfail(reason="Test not yet written", raises=AssertionError, strict=True)
    def test_should_skip_imported_callable(self):
        assert False  # TODO: write this test

    def test_should_detect_xcom_push_tasks(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        def task_func():
            print("bupkis")
            return "done"

        def aux_func():
            return 2 + 2

        def another_func():
            print

        # TODO: detect a naked return statement and don't detect it as an xcom push
        # TODO: detect function inputs as xcom pulls when appropriate

        local_task = PythonOperator(task_id="local_task", python_callable=task_func)
        aux_task = PythonOperator(task_id="aux_task", python_callable=aux_func)
        another_task = PythonOperator(task_id="another_task", python_callable=another_func)
        """
        ast = astroid.parse(test_code)

        local_task_spec = PythonOperatorSpec(ast.body[4].value, "task_func")
        aux_task_spec = PythonOperatorSpec(ast.body[5].value, "aux_func")
        another_task_spec = PythonOperatorSpec(ast.body[6].value, "another_func")
        test_task_ids_to_python_callable_specs = {
            "local_task": local_task_spec,
            "aux_task": aux_task_spec,
            "another_task": another_task_spec,
        }

        result = get_xcoms_from_tasks(ast, test_task_ids_to_python_callable_specs)

        expected_result = (
            {  # xcom pushes
                "local_task": local_task_spec,
                "aux_task": aux_task_spec,
            },
            set(),  # no xcom_pulls
        )

        assert result == expected_result

    def test_should_detect_xcom_pull_tasks(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        def push_func(task_instance, **_):
            print("bupkis")
            return "done"

        def pull_func():
            push_val = task_instance.xcom_pull(task_ids="push_task")

        def pull_again_func():
            print(task_instance.xcom_pull(task_ids="push_task"))

        push_task = PythonOperator(task_id="push_task", python_callable=push_func)
        pull_task = PythonOperator(task_id="pull_task", python_callable=pull_func, provide_context=True)
        pull_again_task = PythonOperator(task_id="pull_again_task", python_callable=pull_again_func, provide_context=True)
        """
        ast = astroid.parse(test_code)

        local_task_spec = PythonOperatorSpec(ast.body[4].value, "push_func")
        aux_task_spec = PythonOperatorSpec(ast.body[5].value, "pull_func")
        another_task_spec = PythonOperatorSpec(ast.body[6].value, "pull_again_func")
        test_task_ids_to_python_callable_specs = {
            "push_task": local_task_spec,
            "pull_task": aux_task_spec,
            "pull_again_task": another_task_spec,
        }

        result = get_xcoms_from_tasks(ast, test_task_ids_to_python_callable_specs)

        expected_result = (
            {
                "push_task": local_task_spec,
            },
            {"push_task"},
        )

        assert result == expected_result


class TestXComChecker(CheckerTestCase):
    """Tests for the XCom checker."""

    CHECKER_CLASS = pylint_airflow.checkers.xcom.XComChecker

    def test_used_xcom(self):
        """Test valid case: _pushtask() returns a value and _pulltask pulls and uses it."""
        test_code = """
        from airflow.operators.python_operator import PythonOperator
        
        def _pushtask():
            print("do stuff")
            return "foobar"
        
        pushtask = PythonOperator(task_id="pushtask", python_callable=_pushtask)
            
        def _pulltask(task_instance, **_):
            print(task_instance.xcom_pull(task_ids="pushtask"))
            
        pulltask = PythonOperator(task_id="pulltask", python_callable=_pulltask, provide_context=True)
        """
        ast = astroid.parse(test_code)
        with self.assertNoMessages():
            self.checker.visit_module(ast)

    def test_unused_xcom(self):
        """Test invalid case: _pushtask() returns a value but it's never used."""
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        def _pushtask():
            print("do stuff")
            return "foobar"

        pushtask = PythonOperator(task_id="pushtask", python_callable=_pushtask)

        def _pulltask():
            print("foobar")

        pulltask = PythonOperator(task_id="pulltask", python_callable=_pulltask)
        """
        ast = astroid.parse(test_code)
        expected_msg_node = ast.body[2].value
        expected_args = "_pushtask"
        with self.assertAddsMessages(
            MessageTest(msg_id="unused-xcom", node=expected_msg_node, args=expected_args),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)


class TestCheckUnusedXComs(CheckerTestCase):
    """Tests for the XCom checker."""

    CHECKER_CLASS = pylint_airflow.checkers.xcom.XComChecker

    def test_empty_inputs_should_not_message(self):
        test_xcoms_pushed = {}
        test_xcoms_pulled_taskids = set()

        with self.assertNoMessages():
            self.checker.check_unused_xcoms(test_xcoms_pushed, test_xcoms_pulled_taskids)

    def test_all_xcoms_used_should_not_message(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        pushtask = PythonOperator(task_id="pushtask", python_callable=_pushtask)
        # further code omitted as not necessary for the test
        """
        ast = astroid.parse(test_code)
        push_call = ast.body[1].value

        test_xcoms_pushed = {"pushtask": PythonOperatorSpec(push_call, "_pushtask")}
        test_xcoms_pulled_taskids = {"pushtask"}

        with self.assertNoMessages():
            self.checker.check_unused_xcoms(test_xcoms_pushed, test_xcoms_pulled_taskids)

    def test_xcoms_not_used_should_not_message(self):
        test_code = """
        from airflow.operators.python_operator import PythonOperator

        pushtask_1 = PythonOperator(task_id="pushtask_1", python_callable=_pushtask_1)
        pushtask_2 = PythonOperator(task_id="pushtask_2", python_callable=_pushtask_2)
        # further code omitted as not necessary for the test
        """
        ast = astroid.parse(test_code)
        push_call_1 = ast.body[1].value
        push_call_2 = ast.body[2].value

        test_xcoms_pushed = {
            "pushtask_1": PythonOperatorSpec(push_call_1, "_pushtask_1"),
            "pushtask_2": PythonOperatorSpec(push_call_2, "_pushtask_2"),
        }
        test_xcoms_pulled_taskids = set()

        with self.assertAddsMessages(
            MessageTest(msg_id="unused-xcom", node=push_call_1, args="_pushtask_1"),
            MessageTest(msg_id="unused-xcom", node=push_call_2, args="_pushtask_2"),
            ignore_position=True,
        ):
            self.checker.check_unused_xcoms(test_xcoms_pushed, test_xcoms_pulled_taskids)
