"""Checks on Airflow XComs."""
from typing import Set, Dict, Tuple

import astroid
from pylint import checkers
from pylint.checkers import utils

from pylint_airflow.__pkginfo__ import BASE_ID

XCOM_CHECKER_MSGS = {
    f"R{BASE_ID}00": (
        "Return value from %s is stored as XCom but not used anywhere",
        "unused-xcom",
        "Return values from a python_callable function or execute() method are "
        "automatically pushed as XCom.",
    )
    # TODO: add a check for pulling XComs that were never pushed
    # TODO: make unused-xcom check sensitive to multiple pushes in the same callable
}


def get_task_ids_to_python_callable_specs(
    node: astroid.Module,
) -> Dict[str, Tuple[astroid.Call, str]]:
    """Fill this in"""
    assign_nodes = [n for n in node.body if isinstance(n, astroid.Assign)]
    call_nodes = [n.value for n in assign_nodes if isinstance(n.value, astroid.Call)]

    # Store nodes containing python_callable arg as:
    # {task_id: (call node, python_callable func name)}
    task_ids_to_python_callable_specs = {}
    for call_node in call_nodes:
        if call_node.keywords:
            task_id = ""
            python_callable = ""
            for keyword in call_node.keywords:
                if keyword.arg == "python_callable":
                    python_callable = keyword.value.name
                    continue
                if keyword.arg == "task_id":
                    task_id = keyword.value.value

            if python_callable:
                task_ids_to_python_callable_specs[task_id] = (call_node, python_callable)

    return task_ids_to_python_callable_specs


def get_xcoms_from_tasks(
    node: astroid.Module, task_ids_to_python_callable_specs: Dict[str, Tuple[astroid.Call, str]]
) -> Tuple[Dict[str, Tuple[astroid.Call, str]], Set[str]]:
    """Now fetch the functions mentioned by python_callable args"""
    xcoms_pushed = {}
    xcoms_pulled_taskids = set()
    for (
        task_id,
        (python_callable, callable_func_name),
    ) in task_ids_to_python_callable_specs.items():
        if callable_func_name == "<lambda>":  # TODO support lambdas
            continue

        callable_func = node.getattr(callable_func_name)[0]

        if not isinstance(callable_func, astroid.FunctionDef):
            continue  # Callable_func is str not FunctionDef when imported

        callable_func = node.getattr(callable_func_name)[0]

        # Check if the function returns any values
        if any(isinstance(n, astroid.Return) for n in callable_func.body):
            # Found a return statement
            xcoms_pushed[task_id] = (python_callable, callable_func_name)

        # Check if the function pulls any XComs
        callable_func_calls = callable_func.nodes_of_class(astroid.Call)
        for callable_func_call in callable_func_calls:
            if (
                isinstance(callable_func_call.func, astroid.Attribute)
                and callable_func_call.func.attrname == "xcom_pull"
            ):
                for keyword in callable_func_call.keywords:
                    if keyword.arg == "task_ids":
                        xcoms_pulled_taskids.add(keyword.value.value)

    return xcoms_pushed, xcoms_pulled_taskids


class XComChecker(checkers.BaseChecker):
    """Checks on Airflow XComs."""

    msgs = XCOM_CHECKER_MSGS

    @utils.only_required_for_messages("unused-xcom")
    def visit_module(self, node: astroid.Module):
        """
        Check for unused XComs.
        XComs can be set (pushed) implicitly via return of a python_callable or
        execute() of an operator. And explicitly by calling xcom_push().

        Currently, this only checks unused XComs from return value of a python_callable.
        """
        python_callable_nodes = get_task_ids_to_python_callable_specs(node)

        # Now fetch the functions mentioned by python_callable args
        xcoms_pushed, xcoms_pulled_taskids = get_xcoms_from_tasks(node, python_callable_nodes)

        self.check_unused_xcoms(xcoms_pushed, xcoms_pulled_taskids)

    def check_unused_xcoms(
        self, xcoms_pushed: Dict[str, Tuple[astroid.Call, str]], xcoms_pulled_taskids: Set[str]
    ):
        """Adds a message for every key in the xcoms_pushed dictionary that is not present in
        xcoms_pulled_taskids. Note that this check does _not_ flag IDs in xcoms_pulled_taskids
        that are not present in the xcoms_pushed dictionary."""
        remainder = xcoms_pushed.keys() - xcoms_pulled_taskids
        if remainder:
            # There's a remainder in xcoms_pushed_taskids which should've been xcom_pulled.
            sorted_remainder = sorted(list(remainder))  # guarantee repeatable ordering of messages
            for remainder_task_id in sorted_remainder:
                python_callable, callable_func_name = xcoms_pushed[remainder_task_id]
                self.add_message("unused-xcom", node=python_callable, args=callable_func_name)
