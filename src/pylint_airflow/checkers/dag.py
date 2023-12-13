"""Checks on Airflow DAGs."""

from collections import defaultdict, OrderedDict
from typing import Tuple, Union, Dict, List

import astroid
from pylint import checkers
from pylint.checkers import utils
from pylint.checkers.utils import safe_infer

from pylint_airflow.__pkginfo__ import BASE_ID


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
    def _find_dag_in_call_node(  # TODO: the func argument can be replaced by call_node.func
        call_node: astroid.Call, func: Union[astroid.Name, astroid.Attribute]
    ) -> Tuple[Union[str, None], Union[astroid.Call, None]]:
        """
        Find DAG in a call_node. Returns (None, None) if no DAG is found.
        :param call_node:
        :param func:
        :return: (dag_id: str, node: astroid.Call)
        :rtype: Tuple  TODO: replace tuple with dataclass
        """
        # check for both 'DAG(dag_id="mydag")' and e.g. 'models.DAG(dag_id="mydag")'
        if (hasattr(func, "name") and func.name == "DAG") or (  # TODO: use type checks
            hasattr(func, "attrname") and func.attrname == "DAG"
        ):
            function_node = safe_infer(func)
            if function_node.is_subtype_of("airflow.models.DAG") or function_node.is_subtype_of(
                "airflow.models.dag.DAG"  # TODO: are both of these subtypes relevant?
            ):
                # Check for "dag_id" as keyword arg
                if call_node.keywords is not None:  # TODO: can just use 'is not'?
                    for keyword in call_node.keywords:
                        # Only constants supported
                        if keyword.arg == "dag_id" and isinstance(keyword.value, astroid.Const):
                            return str(keyword.value.value), call_node

                # Check for dag_id as positional arg
                if call_node.args:  # TODO: unify this with keyword code above
                    if not hasattr(call_node.args[0], "value"):
                        # TODO Support dynamic dag_id. If dag_id is set from variable, it has no value attr.  # pylint: disable=line-too-long
                        return None, None
                    return call_node.args[0].value, call_node

        return None, None

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
        assign_nodes = node.nodes_of_class(astroid.Assign)
        with_nodes = node.nodes_of_class(astroid.With)

        self.find_dags_in_assignments(assign_nodes, dagids_to_nodes)
        self.find_dags_in_context_managers(with_nodes, dagids_to_nodes)

        self.check_single_dag_equals_filename(node, dagids_to_nodes)
        self.check_duplicate_dag_names(dagids_to_nodes)

    def find_dags_in_assignments(self, assign_nodes, dagids_nodes) -> None:
        """Finds calls to DAG constructors in Assign nodes and puts them in the
        dagids_nodes dict."""
        for assign_node in assign_nodes:
            if isinstance(assign_node.value, astroid.Call):
                func = assign_node.value.func
                dagid, dagnode = self._find_dag_in_call_node(assign_node.value, func)
                if dagid and dagnode:  # Checks if there are no Nones
                    dagids_nodes[dagid].append(dagnode)

    def find_dags_in_context_managers(self, with_nodes, dagids_nodes) -> None:
        """Finds calls to DAG constructors in With nodes (context managers) and puts them in the
        dagids_nodes dict."""
        for with_node in with_nodes:
            for with_item in with_node.items:
                call_node = with_item[0]
                if isinstance(call_node, astroid.Call):
                    func = call_node.func
                    dagid, dagnode = self._find_dag_in_call_node(call_node, func)
                    if dagid and dagnode:  # Checks if there are no Nones
                        dagids_nodes[dagid].append(dagnode)

    def check_single_dag_equals_filename(self, node, dagids_to_nodes) -> None:  # TODO: type this
        """Adds a message if the module declares a single DAG AND the dag_id does not match the
        module filename."""
        # Check if single DAG and if equals filename
        # Unit test nodes have file "<?>"
        if len(dagids_to_nodes) == 1 and node.file != "<?>":
            dagid = list(self._dagids_to_deduplicated_nodes(dagids_to_nodes).items())[0]
            expected_filename = f"{dagid}.py"
            current_filename = node.file.split("/")[-1]
            if expected_filename != current_filename:
                self.add_message("match-dagid-filename", node=node)

    def check_duplicate_dag_names(self, dagids_to_nodes) -> None:
        """Adds a message if the module declares two or more DAGs with the same dag_id."""
        duplicate_dagids = [
            (dagid, nodes)
            for dagid, nodes in self._dagids_to_deduplicated_nodes(dagids_to_nodes).items()
            if len(nodes) >= 2 and dagid is not None
        ]
        for (dagid, assign_nodes) in duplicate_dagids:
            self.add_message("duplicate-dag-name", node=assign_nodes[-1], args=dagid)
