"""Tests for the DAG checker."""

import astroid
import pytest
from pylint.testutils import CheckerTestCase, MessageTest

import pylint_airflow
from pylint_airflow.checkers.dag import DagChecker


class TestDuplicateDagName(CheckerTestCase):
    """Tests for the duplicate-dag-name checker."""

    CHECKER_CLASS = pylint_airflow.checkers.dag.DagChecker

    def test_constructed_dags_from_assignment_should_message(self):
        """Test for multiple DAG instances with identical names; here we test multiple ways of
        importing the DAG constructor."""
        testcase = """
        from airflow import models
        from airflow.models import DAG
        import airflow

        # keyword args
        dag1 = DAG(dag_id="mydag")
        dag2 = DAG(dag_id="mydag")

        dag3 = models.DAG(dag_id="lintme")
        dag4 = DAG(dag_id="lintme")

        dag5 = airflow.DAG(dag_id="testme")
        dag6 = DAG(dag_id="testme")

        # positional args
        dag7 = DAG("mydag")
        dag8 = DAG("mydag")

        dag9 = models.DAG("lintme")
        dag10 = DAG("lintme")

        dag11 = airflow.DAG("testme")
        dag12 = DAG("testme")
        """
        ast = astroid.parse(testcase)
        expected_msg_node_1 = ast.body[4].value
        expected_msg_node_2 = ast.body[6].value
        expected_msg_node_3 = ast.body[8].value
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_1, args="mydag"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_2, args="lintme"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_3, args="testme"),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)

    @pytest.mark.xfail(reason="Not yet implemented", raises=AssertionError, strict=True)
    def test_constructed_dags_from_call_should_message(self):
        """Test for multiple DAG instances with identical names; here we test multiple ways of
        importing the DAG constructor."""
        testcase = """
        from airflow import models
        from airflow.models import DAG
        import airflow

        DAG(dag_id="mydag")
        DAG(dag_id="mydag")

        models.DAG(dag_id="lintme")
        DAG(dag_id="lintme")
        
        airflow.DAG(dag_id="testme")
        DAG(dag_id="testme")
        """
        ast = astroid.parse(testcase)
        expected_msg_node_1 = ast.body[4].value
        expected_msg_node_2 = ast.body[6].value
        expected_msg_node_3 = ast.body[8].value
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_1, args="mydag"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_2, args="lintme"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_3, args="testme"),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)

    def test_constructed_dags_from_context_manager_should_message(self):
        """Test for multiple DAG instances with identical names; here we test multiple ways of
        importing the DAG constructor."""
        testcase = """
        from airflow import models
        from airflow.models import DAG
        import airflow

        with DAG(dag_id="mydag"):
            pass
        with DAG(dag_id="mydag"):
            pass

        with models.DAG(dag_id="lintme"):
            pass
        with DAG(dag_id="lintme"):
            pass

        with airflow.DAG(dag_id="testme"):
            pass
        with DAG(dag_id="testme"):
            pass
        """
        ast = astroid.parse(testcase)
        expected_msg_node_1 = ast.body[4].items[0][0]
        expected_msg_node_2 = ast.body[6].items[0][0]
        expected_msg_node_3 = ast.body[8].items[0][0]
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_1, args="mydag"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_2, args="lintme"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_3, args="testme"),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)

    @pytest.mark.xfail(reason="Not yet implemented", raises=AssertionError, strict=True)
    def test_duplicate_dags_should_message_once_for_each_duplicate(self):
        """Test for multiple DAG instances with identical names; here we test all three ways of
        importing the DAG constructor."""
        testcase = """
        from airflow.models import DAG

        dag1 = DAG(dag_id="mydag")
        DAG(dag_id="mydag")
        with DAG(dag_id="mydag"):
            pass
        """
        ast = astroid.parse(testcase)
        expected_msg_node_1 = ast.body[2].value
        expected_msg_node_2 = ast.body[3].items[0][0]
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_1, args="mydag"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_2, args="mydag"),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)

    @pytest.mark.xfail(reason="Not yet implemented", raises=AssertionError, strict=True)
    def test_duplicate_dag_id_from_variable_should_message(self):
        """Test for multiple DAG instances with identical names."""
        testcase = """
        from airflow.models import DAG

        dagname = "mydag"

        dag1 = DAG(dag_id=dagname)
        dag2 = DAG(dag_id="mydag")
        """
        ast = astroid.parse(testcase)
        expected_msg_node = ast.body[3].value
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node, args="mydag"),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)

    @pytest.mark.xfail(reason="Not yet implemented", raises=AssertionError, strict=True)
    def test_duplicate_dag_id_from_f_string_should_message(self):
        """Test for multiple DAG instances with identical names."""
        testcase = """
        from airflow.models import DAG

        dagname = "mydag"

        dag1 = DAG(dag_id="mydagfoo")
        dag2 = DAG(dag_id=f"{dagname}foo")
        """
        ast = astroid.parse(testcase)
        expected_msg_node = ast.body[3].value
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node, args="mydag"),
            ignore_position=True,
        ):
            self.checker.visit_module(ast)

    def test_no_duplicate_dag(self):
        """Test for multiple DAG instances without identical names - this should be fine."""
        testcase = """
        from airflow.models import DAG

        dag = DAG(dag_id="mydag")
        dag2 = DAG(dag_id="lintme")
        dag3 = DAG(dag_id="testme")
        """
        ast = astroid.parse(testcase)
        with self.assertNoMessages():
            self.checker.visit_module(ast)


class TestDagIdsToDeduplicatedNodes:
    def test_empty_input_returns_empty_output(self):
        result = DagChecker._dagids_to_deduplicated_nodes({})

        assert result == {}

    def test_unduplicated_values_return_unchanged(self):
        call_1 = astroid.Call(lineno=0, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        call_2 = astroid.Call(lineno=1, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        call_3 = astroid.Call(lineno=2, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        call_4 = astroid.Call(lineno=3, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        test_dict = {"dag_1": [call_1, call_2], "dag_2": [call_3, call_4]}

        result = DagChecker._dagids_to_deduplicated_nodes(test_dict)

        assert result == test_dict

    def test_duplicated_values_deduplicate_with_left_priority(self):
        """ "Left priority" means we keep the leftmost instance of a duplicated entry."""
        call_1 = astroid.Call(lineno=0, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        call_2 = astroid.Call(lineno=1, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        call_3 = astroid.Call(lineno=2, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        call_4 = astroid.Call(lineno=3, col_offset=0, parent=None, end_lineno=0, end_col_offset=1)
        test_dict = {"dag_1": [call_1, call_2, call_1], "dag_2": [call_3, call_4, call_4]}

        result = DagChecker._dagids_to_deduplicated_nodes(test_dict)

        expected_result = {"dag_1": [call_1, call_2], "dag_2": [call_3, call_4]}
        assert result == expected_result


class TestFindDagInCallNode:
    def test_non_dag_name_call_returns_none(self):
        test_func = astroid.Name(
            name="my_func", lineno=0, col_offset=0, parent=None, end_lineno=0, end_col_offset=1
        )

        test_call = astroid.Call(
            lineno=0, col_offset=0, parent=None, end_lineno=0, end_col_offset=1
        )
        test_call.postinit(func=test_func, args=[], keywords=[])

        result = DagChecker._find_dag_in_call_node(test_call, test_func)

        assert result == (None, None)

    def test_non_dag_attribute_call_returns_none(self):

        test_func = astroid.Attribute(
            attrname="my.other_func",
            lineno=0,
            col_offset=0,
            parent=None,
            end_lineno=0,
            end_col_offset=1,
        )

        test_call = astroid.Call(
            lineno=0, col_offset=0, parent=None, end_lineno=0, end_col_offset=1
        )
        test_call.postinit(func=test_func, args=[], keywords=[])

        result = DagChecker._find_dag_in_call_node(test_call, test_func)

        assert result == (None, None)

    def test_dag_constructor_call_returns(self):
        test_module = astroid.Module(name="test_module")
        test_expr = astroid.Expr(
            lineno=0, col_offset=0, parent=test_module, end_lineno=0, end_col_offset=1
        )
        test_call = astroid.Call(
            lineno=0, col_offset=0, parent=test_expr, end_lineno=0, end_col_offset=1
        )
        test_func = astroid.Name(
            name="list", lineno=0, col_offset=0, parent=test_call, end_lineno=0, end_col_offset=1
        )

        test_call.postinit(func=test_func, args=[], keywords=[])
        test_expr.postinit(test_call)
        test_module.postinit(body=[test_expr])

        result = DagChecker._find_dag_in_call_node(test_call, test_func)

        assert result == (None, None)
