# pylint: disable=missing-function-docstring
"""Tests for the XCom checker and its helper functions."""

import astroid
from pylint.testutils import CheckerTestCase, MessageTest

import pylint_airflow


class TestXComChecker(CheckerTestCase):
    """Tests for the XCom checker."""

    CHECKER_CLASS = pylint_airflow.checkers.xcom.XComChecker

    def test_used_xcom(self):
        """Test valid case: _pushtask() returns a value and _pulltask pulls and uses it."""
        testcase = """
        from airflow.operators.python_operator import PythonOperator
        
        def _pushtask():
            print("do stuff")
            return "foobar"
        
        pushtask = PythonOperator(task_id="pushtask", python_callable=_pushtask)
            
        def _pulltask(task_instance, **_):
            print(task_instance.xcom_pull(task_ids="pushtask"))
            
        pulltask = PythonOperator(task_id="pulltask", python_callable=_pulltask, provide_context=True)
        """
        ast = astroid.parse(testcase)
        with self.assertNoMessages():
            self.checker.visit_module(ast)

    def test_unused_xcom(self):
        """Test invalid case: _pushtask() returns a value but it's never used."""
        testcase = """
        from airflow.operators.python_operator import PythonOperator

        def _pushtask():
            print("do stuff")
            return "foobar"

        pushtask = PythonOperator(task_id="pushtask", python_callable=_pushtask)

        def _pulltask():
            print("foobar")

        pulltask = PythonOperator(task_id="pulltask", python_callable=_pulltask)
        """
        ast = astroid.parse(testcase)
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

        def _pushtask():
            print("do stuff")
            return "foobar"

        pushtask = PythonOperator(task_id="pushtask", python_callable=_pushtask)

        def _pulltask(task_instance, **_):
            print(task_instance.xcom_pull(task_ids="pushtask"))

        pulltask = PythonOperator(task_id="pulltask", python_callable=_pulltask, provide_context=True)
        """
        ast = astroid.parse(test_code)
        push_call = ast.body[2].value

        test_xcoms_pushed = {"pushtask": (push_call, "_pushtask")}
        test_xcoms_pulled_taskids = {"pushtask"}

        with self.assertNoMessages():
            self.checker.check_unused_xcoms(test_xcoms_pushed, test_xcoms_pulled_taskids)
