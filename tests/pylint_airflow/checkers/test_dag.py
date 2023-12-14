# pylint: disable=missing-function-docstring
"""Tests for the DAG checker."""
from collections import defaultdict
from typing import Dict, List

import astroid
import pytest
from pylint.testutils import CheckerTestCase, MessageTest

import pylint_airflow
from pylint_airflow.checkers.dag import DagCallNode, DagChecker


@pytest.fixture(name="test_dagids_to_nodes")
def test_dagids_to_nodes_fixture() -> Dict[str, List[astroid.Call]]:
    return defaultdict(list)


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


class TestFindDagInCallNodeHelper:  # pylint: disable=protected-access,missing-function-docstring
    """Test the _find_dag_in_call_node static helper function.

    There are a lot of conditions to test, so we parameterize the happy and failure paths,
    and write additional test sets for features that haven't yet been written (they fail now,
    but they should eventually be moved to the happy path tests).
    """

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

        result = DagChecker._find_dag_in_call_node(test_call)

        assert result == DagCallNode("my_dag", test_call)

    @pytest.mark.parametrize(
        "test_statement",
        [
            "list([1, 2, 3])",
            "import datetime\n        datetime.date()",
            "DAG()",
            "models.DAG()",
            'DAG(dag_attr="some_data")',
            'models.DAG(dag_attr="some_data")',
            "bupkus()",
        ],
        ids=[
            "Non-DAG Name call",
            "Non-DAG Attribute call",
            "DAG Name call with no arguments",
            "DAG Attribute call with no arguments",
            "DAG Name call with no dag_id keyword argument",
            "DAG Attribute call no dag_id keyword argument",
            "Nonsense unbound function name",
        ],
    )
    def test_invalid_nodes_should_return_none(self, test_statement):
        test_code = f"""
        from airflow import models
        from airflow.models import DAG

        {test_statement}  #@
        """
        test_call = astroid.extract_node(test_code)

        result = DagChecker._find_dag_in_call_node(test_call)

        assert result is None

    @pytest.mark.parametrize(
        "test_statement",
        [
            'test_id = "my_dag"\n        DAG(dag_id=test_id)',
            'test_id = "my_dag"\n        models.DAG(dag_id=test_id)',
            'test_id = "my_dag"\n        DAG(test_id)',
            'test_id = "my_dag"\n        models.DAG(test_id)',
            'test_id = "my_dag"\n        DAG(dag_id=f"{test_id}_0")',
            'test_id = "my_dag"\n        models.DAG(dag_id=f"{test_id}_0")',
            'test_id = "my_dag"\n        DAG(f"{test_id}_0")',
            'test_id = "my_dag"\n        models.DAG(f"{test_id}_0")',
            'test_id = "my_dag"\n        my_id = f"{test_id}_0"\n        DAG(my_id=f"{test_id}_0")',
            'test_id = "my_dag"\n        my_id = f"{test_id}_0"\n        models.DAG(my_id=f"{test_id}_0")',  # pylint: disable=line-too-long
            'test_id = "my_dag"\n        my_id = f"{test_id}_0"\n        DAG(f"{test_id}_0")',
            'test_id = "my_dag"\n        my_id = f"{test_id}_0"\n        models.DAG(f"{test_id}_0")',  # pylint: disable=line-too-long
            "bupkus()",
        ],
        ids=[
            "DAG Name call with variable dag_id keyword argument",
            "DAG Attribute call with variable dag_id keyword argument",
            "DAG Name call with variable dag_id positional argument",
            "DAG Attribute call with variable dag_id positional argument",
            "DAG Name call with f-string dag_id keyword argument",
            "DAG Attribute call with f-string dag_id keyword argument",
            "DAG Name call with f-string dag_id positional argument",
            "DAG Attribute call with f-string dag_id positional argument",
            "DAG Name call with double-variable dag_id keyword argument",
            "DAG Attribute call with double-variable dag_id keyword argument",
            "DAG Name call with double-variable dag_id positional argument",
            "DAG Attribute call with double-variable dag_id positional argument",
            "Nonsense unbound function name",
        ],
    )
    @pytest.mark.xfail(reason="Not yet implemented", raises=AssertionError, strict=True)
    def test_future_work_valid_dag_call_should_return_dag_id_and_node(self, test_statement):
        test_code = f"""
        from airflow import models
        from airflow.models import DAG

        {test_statement}  #@
        """

        test_call = astroid.extract_node(test_code)

        result = DagChecker._find_dag_in_call_node(test_call)

        assert result == ("my_dag", test_call)

    def test_future_work_unimported_dag_module_should_return_none(self):
        """If the code calls a DAG constructor but hasn't imported the appropriate module,
        node inference will fail and return None; we must None-check the inferred node before
        performing the type check, to avoid an AttributeError that blows up the checker call.
        """
        test_code = """DAG("my_dag")  #@"""

        test_call = astroid.extract_node(test_code)

        result = DagChecker._find_dag_in_call_node(test_call)

        assert result is None


class TestDagIdsToDeduplicatedNodesHelper:  # pylint: disable=protected-access,missing-function-docstring
    """Test the _dagids_to_deduplicated_nodes static helper function."""

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


class TestFindDagsInAssignments(CheckerTestCase):
    """Tests for the method that collects DAGs from Assign nodes."""

    CHECKER_CLASS = pylint_airflow.checkers.dag.DagChecker

    def test_no_nodes_collects_nothing(self, test_dagids_to_nodes):
        test_code = "list([1, 2, 3])"
        test_module = astroid.parse(test_code)

        self.checker.collect_dags_in_assignments(test_module, test_dagids_to_nodes)

        assert test_dagids_to_nodes == {}

    def test_valid_dag_assign_nodes_are_collected_invalid_not_collected(self, test_dagids_to_nodes):
        """Here we test a variety of assignment nodes:
        -Valid DAG assignments, by Name and Attribute (should be collected)
        -Assignment to number (should not be collected)
        -Assignment to string (should not be collected)
        -Assignment to function call (should not be collected)

        We also sprinkle in some call nodes in the code, so we can pre-load the dagids_nodes
        dictionary and verify that:
        -Existing entries for which there are no assignments are untouched by the method
        -Existing entries for which there are new assignments are appended instead of overwritten
        """

        test_code = """
        from airflow import models
        from airflow.models import DAG

        not_a_dag = 5
        not_a_dag_2 = "test_dag"
        not_a_dag_3 = list([1, 2, 3])
        test_dag = DAG(dag_id="test_dag")
        test_dag_2 = DAG(dag_id="test_dag")
        DAG(dag_id="test_dag_2")
        DAG(dag_id="test_dag_3")
        test_dag_3 = DAG(dag_id="test_dag_3")
        test_dag_4 = models.DAG(dag_id="test_dag")
        test_dag_5 = DAG(dag_id="test_dag_5")
        """
        test_module = astroid.parse(test_code)
        test_body = test_module.body

        # Pre-load dictionary with values to ensure proper append/non-overwrite behavior
        test_dagids_to_nodes["test_dag_2"].append(test_body[7].value)
        test_dagids_to_nodes["test_dag_3"].append(test_body[8].value)

        # Prepare expected output
        expected_dagids_to_nodes = {
            "test_dag": [test_body[5].value, test_body[6].value, test_body[10].value],
            "test_dag_2": [test_body[7].value],
            "test_dag_3": [test_body[8].value, test_body[9].value],
            "test_dag_5": [test_body[11].value],
        }

        self.checker.collect_dags_in_assignments(test_module, test_dagids_to_nodes)

        assert test_dagids_to_nodes == expected_dagids_to_nodes


class TestFindDagsInCalls(CheckerTestCase):
    """Tests for the method that collects DAGs from Call nodes."""

    CHECKER_CLASS = pylint_airflow.checkers.dag.DagChecker

    def test_no_nodes_collects_nothing(self, test_dagids_to_nodes):
        test_code = "list([1, 2, 3])"
        test_module = astroid.parse(test_code)

        self.checker.collect_dags_in_calls(test_module, test_dagids_to_nodes)

        assert test_dagids_to_nodes == {}

    def test_valid_dag_call_nodes_are_collected_invalid_not_collected(self, test_dagids_to_nodes):
        # pylint: disable=protected-access
        """Here we test a variety of Call nodes:
        -Valid DAG constructions, by Name and Attribute (should be collected)
        -Non-DAG function call (should not be collected)

        We also sprinkle in some assign nodes in the code, so we can pre-load the dagids_nodes
        dictionary and verify that:
        -Existing entries for which there are no calls are untouched by the method
        -Existing entries for which there are new calls are appended instead of overwritten
        """

        test_code = """
        from airflow import models
        from airflow.models import DAG

        list([1, 2, 3])
        DAG(dag_id="test_dag") #3
        DAG(dag_id="test_dag")
        test_dag_2 = DAG(dag_id="test_dag_2") #5
        test_dag_3 = DAG(dag_id="test_dag_3")
        DAG(dag_id="test_dag_3")
        models.DAG(dag_id="test_dag")
        DAG(dag_id="test_dag_5") #9
        """
        test_module = astroid.parse(test_code)
        test_body = test_module.body

        # Pre-load dictionary with values to ensure proper append/non-overwrite behavior
        test_dagids_to_nodes["test_dag_2"].append(test_body[5].value)
        test_dagids_to_nodes["test_dag_3"].append(test_body[6].value)

        # Prepare expected output
        expected_dagids_to_nodes = {
            "test_dag": [test_body[3].value, test_body[4].value, test_body[8].value],
            "test_dag_2": [test_body[5].value],
            "test_dag_3": [test_body[6].value, test_body[7].value],
            "test_dag_5": [test_body[9].value],
        }

        self.checker.collect_dags_in_calls(test_module, test_dagids_to_nodes)

        assert (
            DagChecker._dagids_to_deduplicated_nodes(test_dagids_to_nodes)
            == expected_dagids_to_nodes
        )


class TestFindDagsInContextManagers(CheckerTestCase):
    """Tests for the method that collects DAGs from With nodes (context manager blocks)."""

    CHECKER_CLASS = pylint_airflow.checkers.dag.DagChecker

    def test_no_nodes_collects_nothing(self, test_dagids_to_nodes):
        test_code = "list([1, 2, 3])"
        test_module = astroid.parse(test_code)

        self.checker.collect_dags_in_context_managers(test_module, test_dagids_to_nodes)

        assert test_dagids_to_nodes == {}

    def test_valid_dag_with_nodes_are_collected_invalid_not_collected(self, test_dagids_to_nodes):
        """Here we test a variety of context manager nodes:
        -Valid DAG blocks, by Name and Attribute (should be collected)
        -Function call (temp dir constructor) by context manager
        -Context manager entering by variable

        We also sprinkle in some call nodes in the code, so we can pre-load the dagids_nodes
        dictionary and verify that:
        -Existing entries for which there are no assignments are untouched by the method
        -Existing entries for which there are new assignments are appended instead of overwritten
        """
        test_code = """
        from airflow import models
        from airflow.models import DAG
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            pass

        fo = open(file="bupkis.txt", mode="wt")
        with fo as fil:
            pass

        with DAG(dag_id="test_dag"):
            pass

        with DAG(dag_id="test_dag"):
            pass

        DAG(dag_id="test_dag_2")
        DAG(dag_id="test_dag_3")

        with DAG(dag_id="test_dag_3") as dag_3, DAG(dag_id="test_dag_4") as dag_4:
            pass

        with models.DAG(dag_id="test_dag"):
            pass

        with DAG(dag_id="test_dag_5"):
            pass
        """
        test_module = astroid.parse(test_code)
        test_body = test_module.body

        # Pre-load dictionary with values to ensure proper append/non-overwrite behavior
        test_dagids_to_nodes["test_dag_2"].append(test_body[8].value)
        test_dagids_to_nodes["test_dag_3"].append(test_body[9].value)

        # Prepare expected output
        expected_dagids_to_nodes = {
            "test_dag": [
                test_body[6].items[0][0],
                test_body[7].items[0][0],
                test_body[11].items[0][0],
            ],
            "test_dag_2": [test_body[8].value],
            "test_dag_3": [test_body[9].value, test_body[10].items[0][0]],
            "test_dag_4": [test_body[10].items[1][0]],
            "test_dag_5": [test_body[12].items[0][0]],
        }

        self.checker.collect_dags_in_context_managers(test_module, test_dagids_to_nodes)

        assert test_dagids_to_nodes == expected_dagids_to_nodes


class TestCheckSingleDagEqualsFilename(CheckerTestCase):
    """Tests for the match-id-filename method. We use the CheckerTestCase because it makes it
    easy to set up the checker instance and verify the expected messages."""

    CHECKER_CLASS = pylint_airflow.checkers.dag.DagChecker

    def test_single_dag_mismatched_filename_should_message(self):
        test_code = """
        from airflow.models import DAG

        test_dag = DAG(dag_id="test_dag")
        """
        test_module = astroid.parse(test_code)
        test_module.file = "my_dag.py"
        dagids_to_nodes = {"test_dag": [test_module.body[1].value]}

        with self.assertAddsMessages(
            MessageTest(msg_id="match-dagid-filename", node=test_module),
            ignore_position=True,
        ):
            self.checker.check_single_dag_equals_filename(
                node=test_module, dagids_to_nodes=dagids_to_nodes
            )

    def test_single_dag_matching_filename_should_not_message(self):
        test_code = """
        from airflow.models import DAG

        test_dag = DAG(dag_id="test_dag")
        """
        test_module = astroid.parse(test_code)
        test_module.file = "test_dag.py"
        dagids_to_nodes = {"test_dag": [test_module.body[1].value]}

        with self.assertNoMessages():
            self.checker.check_single_dag_equals_filename(
                node=test_module, dagids_to_nodes=dagids_to_nodes
            )

    def test_single_dag_with_test_node_filename_should_not_message(self):
        test_code = """
        from airflow.models import DAG

        test_dag = DAG(dag_id="test_dag")
        """
        test_module = astroid.parse(test_code)
        test_module.file = "<?>"
        dagids_to_nodes = {"test_dag": [test_module.body[1].value]}

        with self.assertNoMessages():
            self.checker.check_single_dag_equals_filename(
                node=test_module, dagids_to_nodes=dagids_to_nodes
            )

    def test_zero_dags_should_not_message(self):
        test_code = "list([1, 2, 3])"
        test_module = astroid.parse(test_code)
        test_module.file = "<?>"
        dagids_to_nodes = {}

        with self.assertNoMessages():
            self.checker.check_single_dag_equals_filename(
                node=test_module, dagids_to_nodes=dagids_to_nodes
            )

    def test_multiple_dags_should_not_message(self):
        test_code = """
        from airflow.models import DAG

        test_dag = DAG(dag_id="test_dag")
        test_dag_2 = DAG(dag_id="test_dag_2")
        """
        test_module = astroid.parse(test_code)
        test_module.file = "test_dag.py"
        dagids_to_nodes = {
            "test_dag": [test_module.body[1].value],
            "test_dag_2": [test_module.body[2].value],
        }

        with self.assertNoMessages():
            self.checker.check_single_dag_equals_filename(
                node=test_module, dagids_to_nodes=dagids_to_nodes
            )


class TestCheckDuplicateDagNames(CheckerTestCase):
    """Tests for the duplicate-dag-name method."""

    CHECKER_CLASS = pylint_airflow.checkers.dag.DagChecker

    def test_zero_dags_should_not_message(self):
        with self.assertNoMessages():
            self.checker.check_duplicate_dag_names({})

    def test_duplicate_dags_should_message_singular_dags_should_not(self):
        test_code = """
        from airflow.models import DAG

        test_dag = DAG(dag_id="test_dag")
        test_dag_2 = DAG(dag_id="test_dag_2")
        test_dag_a = DAG(dag_id="test_dag")
        """
        test_module = astroid.parse(test_code)
        dagids_to_nodes = {
            "test_dag": [test_module.body[1].value, test_module.body[3].value],
            "test_dag_2": [test_module.body[2].value],
        }

        with self.assertAddsMessages(
            MessageTest(
                msg_id="duplicate-dag-name", node=test_module.body[3].value, args="test_dag"
            ),
            ignore_position=True,
        ):
            self.checker.check_duplicate_dag_names(dagids_to_nodes)

    def test_multi_duplicate_dag_should_message_multiple_times(self):
        test_code = """
        from airflow.models import DAG

        test_dag = DAG(dag_id="test_dag")
        test_dag_2 = DAG(dag_id="test_dag")
        test_dag_3 = DAG(dag_id="test_dag")
        """
        test_module = astroid.parse(test_code)
        dagids_to_nodes = {
            "test_dag": [
                test_module.body[1].value,
                test_module.body[2].value,
                test_module.body[3].value,
            ],
        }

        with self.assertAddsMessages(
            MessageTest(
                msg_id="duplicate-dag-name", node=test_module.body[2].value, args="test_dag"
            ),
            MessageTest(
                msg_id="duplicate-dag-name", node=test_module.body[3].value, args="test_dag"
            ),
            ignore_position=True,
        ):
            self.checker.check_duplicate_dag_names(dagids_to_nodes)
