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
        dag7 = DAG("mydag_pos")
        dag8 = DAG("mydag_pos")

        dag9 = models.DAG("lintme_pos")
        dag10 = DAG("lintme_pos")

        dag11 = airflow.DAG("testme_pos")
        dag12 = DAG("testme_pos")
        """
        ast = astroid.parse(testcase)
        expected_msg_node_1 = ast.body[4].value
        expected_msg_node_2 = ast.body[6].value
        expected_msg_node_3 = ast.body[8].value
        expected_msg_node_4 = ast.body[10].value
        expected_msg_node_5 = ast.body[12].value
        expected_msg_node_6 = ast.body[14].value
        with self.assertAddsMessages(
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_1, args="mydag"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_2, args="lintme"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_3, args="testme"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_4, args="mydag_pos"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_5, args="lintme_pos"),
            MessageTest(msg_id="duplicate-dag-name", node=expected_msg_node_6, args="testme_pos"),
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


class TestFindDagInCallNodeHelper:
    @pytest.mark.parametrize(
        "test_statement",
        [
            'DAG(dag_id="my_dag")',
            'DAG("my_dag")',
            'models.DAG(dag_id="my_dag")',
            'models.DAG("my_dag")',
        ],
        ids=[
            "Name call w/ keyword arg",
            "Name call w/ positional arg",
            "Attribute call w/ keyword arg",
            "Attribute call w/ positional arg",
        ],
    )
    def test_valid_dag_call_should_return_dag_id_and_node(self, test_statement):
        test_code = f"""
        from airflow import models
        from airflow.models import DAG

        {test_statement}  #@
        """

        test_call = astroid.extract_node(test_code)

        result = DagChecker._find_dag_in_call_node(test_call, test_call.func)

        assert result == ("my_dag", test_call)

    @pytest.mark.parametrize(
        "test_statement",
        [
            "list([1, 2, 3])",
            "import datetime\n        datetime.date()",
            "DAG()",
            "models.DAG()",
            'DAG(dag_attr="some_data")',
            'models.DAG(dag_attr="some_data")',
            'test_id = "my_dag"\n        DAG(dag_id=test_id)',
            'test_id = "my_dag"\n        models.DAG(dag_id=test_id)',
            'test_id = "my_dag"\n        DAG(test_id)',
            'test_id = "my_dag"\n        models.DAG(test_id)',
            'test_id = "my_dag"\n        DAG(f"{test_id}_0")',
            'test_id = "my_dag"\n        models.DAG(f"{test_id}_0")',
        ],
        ids=[
            "Non-DAG Name call",
            "Non-DAG Attribute call",
            "DAG Name call with no arguments",
            "DAG Attribute call with no arguments",
            "DAG Name call with no dag_id keyword argument",
            "DAG Attribute call no dag_id keyword argument",
            "DAG Name call with variable dag_id keyword argument",
            "DAG Attribute call with variable dag_id keyword argument",
            "DAG Name call with variable dag_id positional argument",
            "DAG Attribute call with variable dag_id positional argument",
            "DAG Name call with f-string dag_id positional argument",
            "DAG Attribute call with f-string dag_id positional argument",
        ],
    )
    def test_invalid_nodes_should_return_none(self, test_statement):
        test_code = f"""
        from airflow import models
        from airflow.models import DAG

        {test_statement}  #@
        """
        test_call = astroid.extract_node(test_code)

        result = DagChecker._find_dag_in_call_node(test_call, test_call.func)

        assert result == (None, None)
