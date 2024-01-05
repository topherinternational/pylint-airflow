"""Checks on Airflow XComs.

This module contains the XComChecker class and a collection of functions.

XComChecker contains only:
- Methods interfacing with the pylint checker API (i.e. `visit_<nodetype>()` methods)
- Methods that add pylint messages for rules violations (`check_<message>()`)

The module-level functions perform any work that isn't a pylint checker method or adding a message.
"""
from dataclasses import dataclass
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


@dataclass
class PythonOperatorSpec:
    """Data class to hold the call (constructor) node for a PythonOperator construction and
    the name of the function passed as the python_callable argument."""

    python_operator_call_node: astroid.Call
    python_callable_function_name: str


def get_task_ids_to_python_callable_specs(node: astroid.Module) -> Dict[str, PythonOperatorSpec]:
    # pylint: disable=too-many-nested-blocks
    """Fill this in"""
    assign_nodes = node.nodes_of_class(astroid.Assign)
    call_nodes = [assign.value for assign in assign_nodes if isinstance(assign.value, astroid.Call)]

    # Store nodes containing python_callable arg as:
    # {task_id: PythonOperatorSpec(call node, python_callable func name)}

    task_ids_to_python_callable_specs = {}
    for call_node in call_nodes:
        if call_node.keywords:
            task_id = ""
            python_callable_name = ""
            for keyword in call_node.keywords:
                kw_value = keyword.value
                if keyword.arg == "python_callable":
                    if isinstance(kw_value, astroid.Name):  # TODO: support lambdas
                        python_callable_name = kw_value.name
                    elif isinstance(kw_value, astroid.Attribute):
                        if isinstance(kw_value.expr, astroid.Name):
                            python_callable_name = f"{kw_value.expr.name}.{kw_value.attrname}"
                elif keyword.arg == "task_id" and isinstance(kw_value, astroid.Const):
                    task_id = kw_value.value  # TODO: support non-Const args

            if python_callable_name:
                task_ids_to_python_callable_specs[task_id] = PythonOperatorSpec(
                    call_node, python_callable_name
                )

    return task_ids_to_python_callable_specs


def get_xcoms_from_tasks(
    node: astroid.Module, task_ids_to_python_callable_specs: Dict[str, PythonOperatorSpec]
) -> Tuple[Dict[str, PythonOperatorSpec], Set[str]]:
    """Now fetch the functions mentioned by python_callable args"""
    xcoms_pushed = {}
    xcoms_pulled_taskids = set()

    for task_id, python_operator_spec in task_ids_to_python_callable_specs.items():
        callable_func_name = python_operator_spec.python_callable_function_name
        if callable_func_name == "<lambda>":  # TODO support lambdas
            continue

        callable_func = node.getattr(callable_func_name)[0]
        # ^ TODO: handle builtins and attribute imports that will raise on this call

        if not isinstance(callable_func, astroid.FunctionDef):
            continue  # Callable_func is str not FunctionDef when imported

        # Check if the function returns any values
        if any(isinstance(statement, astroid.Return) for statement in callable_func.body):
            # Found a return statement
            xcoms_pushed[task_id] = python_operator_spec

        # Check if the function pulls any XComs
        callable_func_calls = callable_func.nodes_of_class(astroid.Call)
        for callable_func_call in callable_func_calls:
            callable_func = callable_func_call.func
            if (
                isinstance(callable_func, astroid.Attribute)
                and callable_func.attrname == "xcom_pull"
            ):
                for keyword in callable_func_call.keywords:
                    if keyword.arg == "task_ids" and isinstance(keyword.value, astroid.Const):
                        xcoms_pulled_taskids.add(keyword.value.value)
                        # TODO: add support for xcom 'key' argument
                        # TODO: add support for non-Const argument values

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
        xcoms_pushed, xcoms_pulled_taskids = get_xcoms_from_tasks(node, python_callable_nodes)

        self.check_unused_xcoms(xcoms_pushed, xcoms_pulled_taskids)

    def check_unused_xcoms(
        self, xcoms_pushed: Dict[str, PythonOperatorSpec], xcoms_pulled_taskids: Set[str]
    ):
        """Adds a message for every key in the xcoms_pushed dictionary that is not present in
        xcoms_pulled_taskids. Note that this check does _not_ flag IDs in xcoms_pulled_taskids
        that are not present in the xcoms_pushed dictionary."""
        remainder = xcoms_pushed.keys() - xcoms_pulled_taskids
        if remainder:
            # There's a task_id in xcoms_pushed_taskids which should have been xcom_pull'd
            sorted_remainder = sorted(list(remainder))  # guarantee repeatable ordering of messages
            for remainder_task_id in sorted_remainder:
                python_operator_spec = xcoms_pushed[remainder_task_id]
                self.add_message(
                    "unused-xcom",
                    node=python_operator_spec.python_operator_call_node,
                    args=python_operator_spec.python_callable_function_name,
                )
