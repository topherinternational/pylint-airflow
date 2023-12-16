"""Checks on Airflow DAGs."""
from collections import defaultdict, OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional

import astroid
from pylint import checkers
from pylint.checkers import utils
from pylint.checkers.utils import safe_infer

from pylint_airflow.__pkginfo__ import BASE_ID


@dataclass
class DagCallNode:
    """Data class to hold dag_id and a call node returning a DAG with that dag_id"""

    dag_id: str
    call_node: astroid.Call


def dag_call_node_from_const(
    const_node: astroid.Const, call_node: astroid.Call
) -> Optional[DagCallNode]:
    """Returns a DagCallNode instance with dag_id extracted from the const_node argument"""
    return DagCallNode(str(const_node.value), call_node)


def dag_call_node_from_name(
    name_node: astroid.Name, call_node: astroid.Call
) -> Optional[DagCallNode]:
    """Returns a DagCallNode instance with dag_id extracted from the name_node argument,
    or None if the node value can't be extracted."""
    name_val = safe_infer(name_node)  # TODO: follow name chains
    if isinstance(name_val, astroid.Const):
        return dag_call_node_from_const(name_val, call_node)
    return None


def dag_call_node_from_joined_string(
    joined_str_node: astroid.JoinedStr, call_node: astroid.Call
) -> Optional[DagCallNode]:
    """Returns a DagCallNode instance with dag_id composed from the elements of the
    joined_str_node argument, or None if the node value can't be extracted."""
    dag_id_elements: List[str] = []
    for js_value in joined_str_node.values:
        if isinstance(js_value, astroid.FormattedValue):
            inf_val = safe_infer(js_value.value)
            if inf_val and isinstance(inf_val, astroid.Const):
                dag_id_elements.append(str(inf_val.value))
            else:
                return None
        elif isinstance(js_value, astroid.Const):
            dag_id_elements.append(str(js_value.value))
    return DagCallNode("".join(dag_id_elements), call_node)
    # TODO: follow name chains


def dag_call_node_from_argument_value(
    argument_value: astroid.NodeNG, call_node: astroid.Call
) -> Optional[DagCallNode]:
    """Detects argument string from Const, Name or JoinedStr (f-string), or None if no match"""
    if isinstance(argument_value, astroid.Const):
        return dag_call_node_from_const(argument_value, call_node)
    if isinstance(argument_value, astroid.Name):
        return dag_call_node_from_name(argument_value, call_node)
    if isinstance(argument_value, astroid.JoinedStr):
        return dag_call_node_from_joined_string(argument_value, call_node)
    return None


def is_dag_node_by_name_or_attribute(func: astroid.NodeNG) -> bool:
    """Check for either 'DAG(dag_id="mydag")' or e.g. 'models.DAG(dag_id="mydag")'"""
    return (isinstance(func, astroid.Name) and func.name == "DAG") or (
        isinstance(func, astroid.Attribute) and func.attrname == "DAG"
    )


def is_inferred_value_subtype_of_dag(function_node: Optional[astroid.ClassDef]) -> bool:
    """Checks class type against DAG class types"""
    return function_node and (
        function_node.is_subtype_of("airflow.models.DAG")
        or function_node.is_subtype_of("airflow.models.dag.DAG")
        # ^ TODO: are both of these subtypes relevant?
    )


class DagChecker(checkers.BaseChecker):
    """Checks conditions in the context of (a) complete DAG(s)."""

    msgs = {
        f"W{BASE_ID}00": (
            "Don't place BaseHook calls at the top level of DAG script",
            "basehook-top-level",
            "Airflow executes DAG scripts periodically and anything at the top level "
            "of a script is executed. Therefore, move BaseHook calls into "
            "functions/hooks/operators.",
        ),
        f"E{BASE_ID}00": (
            "DAG name %s already used",
            "duplicate-dag-name",
            "DAG name should be unique.",
        ),
        f"E{BASE_ID}01": (
            "Task name {} already used",
            "duplicate-task-name",
            "Task name within a DAG should be unique.",
        ),
        f"E{BASE_ID}02": (
            "Task dependency {}->{} already set",
            "duplicate-dependency",
            "Task dependencies can be defined only once.",
        ),
        f"E{BASE_ID}03": (
            "DAG {} contains cycles",
            "dag-with-cycles",
            "A DAG is acyclic and cannot contain cycles.",
        ),
        f"E{BASE_ID}04": (
            "Task {} is not bound to any DAG instance",
            "task-no-dag",
            "A task must know a DAG instance to run.",
        ),
        f"C{BASE_ID}06": (
            "For consistency match the DAG filename with the dag_id",
            "match-dagid-filename",
            "For consistency match the DAG filename with the dag_id.",
        ),
    }

    @staticmethod
    def _find_dag_in_call_node(call_node: astroid.Call) -> Optional[DagCallNode]:
        """
        Find DAG in a call_node. Returns None if no DAG is found.
        :param call_node:
        :return: DagCallNode of dag_id and call_node
        """

        func = call_node.func

        if not is_dag_node_by_name_or_attribute(func):
            return None

        function_node = safe_infer(func)
        if not is_inferred_value_subtype_of_dag(function_node):
            return None

        # Check for "dag_id" as keyword arg
        if call_node.keywords:
            for keyword in call_node.keywords:
                if keyword.arg == "dag_id":
                    return dag_call_node_from_argument_value(keyword.value, call_node)

        # Check for dag_id as 0-th positional arg if we didn't find the dag_id keyword arg
        if call_node.args:
            return dag_call_node_from_argument_value(call_node.args[0], call_node)

        # if we found neither a keyword arg or a positional arg
        return None

    @staticmethod
    def _dagids_to_deduplicated_nodes(
        dagids_to_nodes: Dict[str, List[astroid.Call]]
    ) -> Dict[str, List[astroid.Call]]:
        """This utility function transforms the dagids_to_nodes dictionary to make its List
        values ordered sets - i.e., the list is pruned of duplicate entries while maintaining
        the original insertion order. This allows the correct duplicate node to be cited by
        messages that detect duplicate uses of the same dag_id."""
        return {
            dag_id: list(OrderedDict.fromkeys(nodes)) for dag_id, nodes in dagids_to_nodes.items()
        }

    @utils.only_required_for_messages("duplicate-dag-name", "match-dagid-filename")
    def visit_module(self, node: astroid.Module):
        """We must peruse an entire module to detect inter-DAG issues."""
        dagids_to_nodes: Dict[str, List[astroid.Call]] = defaultdict(list)

        self.collect_dags_in_assignments(node, dagids_to_nodes)
        self.collect_dags_in_calls(node, dagids_to_nodes)
        self.collect_dags_in_context_managers(node, dagids_to_nodes)

        self.check_single_dag_equals_filename(node, dagids_to_nodes)
        self.check_duplicate_dag_names(dagids_to_nodes)

    def collect_dags_in_assignments(self, module_node: astroid.Module, dagids_nodes) -> None:
        """Finds calls to DAG constructors in Assign nodes and puts them in the
        dagids_nodes dict."""
        assign_nodes = module_node.nodes_of_class(astroid.Assign)
        for assign_node in assign_nodes:
            if isinstance(assign_node.value, astroid.Call):
                dag_call_node = self._find_dag_in_call_node(assign_node.value)
                if dag_call_node:
                    dagids_nodes[dag_call_node.dag_id].append(dag_call_node.call_node)

    def collect_dags_in_calls(self, module_node: astroid.Module, dagids_nodes) -> None:
        """Finds calls to DAG constructors in Call nodes and puts them in the
        dagids_nodes dict."""
        call_nodes = module_node.nodes_of_class(astroid.Call)
        for call_node in call_nodes:
            dag_call_node = self._find_dag_in_call_node(call_node)
            if dag_call_node:
                dagids_nodes[dag_call_node.dag_id].append(dag_call_node.call_node)

    def collect_dags_in_context_managers(self, module_node: astroid.Module, dagids_nodes) -> None:
        """Finds calls to DAG constructors in With nodes (context managers) and puts them in the
        dagids_nodes dict."""
        with_nodes = module_node.nodes_of_class(astroid.With)
        for with_node in with_nodes:
            for with_item in with_node.items:
                call_node = with_item[0]
                if isinstance(call_node, astroid.Call):  # TODO: support non-call args (like vars)
                    dag_call_node = self._find_dag_in_call_node(call_node)
                    if dag_call_node:
                        dagids_nodes[dag_call_node.dag_id].append(dag_call_node.call_node)

    def check_single_dag_equals_filename(
        self, node: astroid.Module, dagids_to_nodes: Dict[str, List[astroid.Call]]
    ) -> None:
        """Adds a message if the module declares a single DAG AND the dag_id does not match the
        module filename."""
        # Check if single DAG and if equals filename
        # Unit test nodes have file "<?>"
        if len(dagids_to_nodes) == 1 and node.file != "<?>":
            dagid = list(self._dagids_to_deduplicated_nodes(dagids_to_nodes).items())[0][0]
            expected_filename = f"{dagid}.py"
            current_filename = node.file.split("/")[-1]
            if expected_filename != current_filename:
                self.add_message("match-dagid-filename", node=node)

    def check_duplicate_dag_names(self, dagids_to_nodes) -> None:
        """Adds a message if the module declares two or more DAGs with the same dag_id."""
        duplicate_dags = [
            (dagid, dag_nodes)
            for dagid, dag_nodes in self._dagids_to_deduplicated_nodes(dagids_to_nodes).items()
            if len(dag_nodes) > 1 and dagid is not None
        ]
        for (dagid, dag_nodes) in duplicate_dags:
            for dag_node in dag_nodes[1:]:  # all nodes except the first one are duplicates
                self.add_message("duplicate-dag-name", node=dag_node, args=dagid)
