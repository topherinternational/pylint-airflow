# pylint: disable=missing-function-docstring
"""Tests for the Operator checker and its helper functions."""
import astroid
import pytest
from pylint.testutils import CheckerTestCase, MessageTest

import pylint_airflow
from pylint_airflow.checkers.operator import TaskParameters


class TestOperatorChecker(CheckerTestCase):
    """Tests for the Operator checker."""

    CHECKER_CLASS = pylint_airflow.checkers.operator.OperatorChecker

    def test_different_operator_varname_taskid(self):
        """task_id and operator instance name should match, but differ so should add message."""
        testcase = """
        from airflow.operators.dummy_operator import DummyOperator
        mytask = DummyOperator(task_id="foo") #@
        """
        expected_message = "different-operator-varname-taskid"

        assign_node = astroid.extract_node(testcase)
        with self.assertAddsMessages(
            MessageTest(msg_id=expected_message, node=assign_node), ignore_position=True
        ):
            self.checker.visit_assign(assign_node)

    def test_different_operator_varname_taskid_baseoperator(self):
        """
        task_id and operator instance name should match, but differ, so should add message, also
        when using BaseOperator.
        """
        testcase = """
        from airflow.models import BaseOperator
        mytask = BaseOperator(task_id="foo") #@
        """
        expected_message = "different-operator-varname-taskid"

        assign_node = astroid.extract_node(testcase)
        with self.assertAddsMessages(
            MessageTest(msg_id=expected_message, node=assign_node), ignore_position=True
        ):
            self.checker.visit_assign(assign_node)

    def test_different_operator_varname_taskid_valid(self):
        """task_id and operator instance name are identical so no message should be added."""
        testcase = """
        from airflow.operators.dummy_operator import DummyOperator
        mytask = DummyOperator(task_id="mytask") #@
        """

        assign_node = astroid.extract_node(testcase)
        with self.assertNoMessages():
            self.checker.visit_assign(assign_node)

    @pytest.mark.parametrize(
        "imports,operator_def",
        [
            (
                "from airflow.operators.python_operator import PythonOperator",
                'mytask = PythonOperator(task_id="mytask", python_callable=foo) #@',
            ),
            (
                "from airflow.operators import python_operator",
                'mytask = python_operator.PythonOperator(task_id="mytask", python_callable=foo) #@',
            ),
            (
                "import airflow.operators.python_operator",
                'mytask = airflow.operators.python_operator.PythonOperator(task_id="mytask", python_callable=foo) #@',  # pylint: disable=line-too-long
            ),
        ],
    )
    def test_match_callable_taskid(self, imports, operator_def):
        """tests matching match_callable_taskid"""
        testcase = f"{imports}\ndef foo(): print('dosomething')\n{operator_def}"
        expected_message = "match-callable-taskid"

        assign_node = astroid.extract_node(testcase)
        with self.assertAddsMessages(
            MessageTest(msg_id=expected_message, node=assign_node), ignore_position=True
        ):
            self.checker.visit_assign(assign_node)

    def test_not_match_callable_taskid(self):
        """python_callable function name matches _[task_id], expect no message."""
        testcase = """
        from airflow.operators.python_operator import PythonOperator

        def _mytask():
            print("dosomething")

        mytask = PythonOperator(task_id="mytask", python_callable=_mytask) #@
        """

        assign_node = astroid.extract_node(testcase)
        with self.assertNoMessages():
            self.checker.visit_assign(assign_node)


class TestCheckOperatorVarnameVersusTaskId(CheckerTestCase):
    """Tests for the match-callable-taskid function."""

    CHECKER_CLASS = pylint_airflow.checkers.operator.OperatorChecker

    def test_varname_does_not_match_task_id_should_message(self):
        test_node = astroid.extract_node("a = 1")
        test_var_name = "my_task_name"
        test_task_id = "my_task_id"

        with self.assertAddsMessages(
            MessageTest(msg_id="different-operator-varname-taskid", node=test_node),
            ignore_position=True,
        ):
            self.checker.check_operator_varname_versus_task_id(
                test_node, TaskParameters(test_var_name, test_task_id)
            )

    def test_varname_matches_task_id_should_not_message(self):
        test_node = astroid.extract_node("a = 1")
        test_name = "my_task"

        with self.assertNoMessages():
            self.checker.check_operator_varname_versus_task_id(
                test_node, TaskParameters(test_name, test_name)
            )

    @pytest.mark.parametrize(
        "test_var_name, test_task_id",
        [
            ("var_name", ""),
            ("var_name", None),
            ("", "task_id"),
            (None, "task_id"),
            (None, None),
            ("", None),
            (None, ""),
            ("", ""),
        ],
    )
    def test_either_argument_is_empty_or_none_should_not_message(self, test_var_name, test_task_id):
        test_node = astroid.extract_node("a = 1")

        with self.assertNoMessages():
            self.checker.check_operator_varname_versus_task_id(
                test_node, TaskParameters(test_var_name, test_task_id)
            )


class TestCheckCallableNameVersusTaskId(CheckerTestCase):
    """Tests for the different-operator-varname-taskid function."""

    CHECKER_CLASS = pylint_airflow.checkers.operator.OperatorChecker

    def test_python_callable_name_does_not_match_underscored_task_id_should_message(self):
        test_node = astroid.extract_node("a = 1")
        test_task_id = "my_task_id"
        test_python_callable_name = "my_task_function"

        with self.assertAddsMessages(
            MessageTest(msg_id="match-callable-taskid", node=test_node), ignore_position=True
        ):
            self.checker.check_callable_name_versus_task_id(
                test_node, TaskParameters(test_task_id, test_task_id, test_python_callable_name)
            )

    def test_python_callable_name_matches_underscored_task_id_should_not_message(self):
        test_node = astroid.extract_node("a = 1")
        test_task_id = "my_task_name"
        test_python_callable_name = "_my_task_name"

        with self.assertNoMessages():
            self.checker.check_callable_name_versus_task_id(
                test_node, TaskParameters(test_task_id, test_task_id, test_python_callable_name)
            )

    @pytest.mark.parametrize(
        "test_task_id, test_python_callable_name",
        [
            ("task_id", ""),
            ("task_id", None),
            ("", "callable_name"),
            (None, "callable_name"),
            (None, None),
            (None, ""),
            ("", None),
            ("", ""),
        ],
    )
    def test_either_argument_is_empty_or_none_should_not_message(
        self, test_python_callable_name, test_task_id
    ):
        test_node = astroid.extract_node("a = 1")

        with self.assertNoMessages():
            self.checker.check_callable_name_versus_task_id(
                test_node, TaskParameters(test_task_id, test_task_id, test_python_callable_name)
            )


class TestCheckMixedDependencyDirections(CheckerTestCase):
    """Tests for the Operator checker."""

    CHECKER_CLASS = pylint_airflow.checkers.operator.OperatorChecker

    @pytest.mark.parametrize(
        "dependencies,expect_msg",
        [
            ("t1 >> t2", False),
            ("t1 >> t2 << t3", True),
            ("t1 >> t2 >> t3 >> t4 >> t5", False),
            ("t1 >> t2 << t3 >> t4 << t5", True),
            ("t1 >> [t2, t3]", False),
            ("[t1, t2] >> t3", False),
            ("[t1, t2] >> t3 << t4", True),
            ("t1 >> t2 << [t3, t4]", True),
            ("[t1, t2] >> t3 << [t4, t5]", True),
        ],
    )
    def test_mixed_dependency_directions(self, dependencies, expect_msg):
        """
        Test various ways (both directions & single task/lists) to set dependencies using bitshift
        operators. Should add message when mixing directions.
        """
        testcase = f"""
        from airflow.operators.dummy_operator import DummyOperator
        t1 = DummyOperator(task_id="t1")
        t2 = DummyOperator(task_id="t2")
        t3 = DummyOperator(task_id="t3")
        t4 = DummyOperator(task_id="t4")
        t5 = DummyOperator(task_id="t5")
        {dependencies} #@
        """
        message = "mixed-dependency-directions"
        binop_node = astroid.extract_node(testcase)

        if expect_msg:
            with self.assertAddsMessages(
                MessageTest(msg_id=message, node=binop_node), ignore_position=True
            ):
                self.checker.check_mixed_dependency_directions(binop_node)
        else:
            with self.assertNoMessages():
                self.checker.check_mixed_dependency_directions(binop_node)
