"""Checks on Airflow operators."""
from typing import Set

import astroid
from pylint import checkers
from pylint.checkers import utils
from pylint.checkers.utils import safe_infer

from pylint_airflow.__pkginfo__ import BASE_ID

OPERATOR_CHECKER_MSGS = {
    f"C{BASE_ID}00": (
        "Operator variable name and task_id argument should match",
        "different-operator-varname-taskid",
        "For consistency assign the same variable name and task_id to operators.",
    ),
    f"C{BASE_ID}01": (
        "Name the python_callable function '_[task_id]'",
        "match-callable-taskid",
        "For consistency name the callable function '_[task_id]', e.g. "
        "PythonOperator(task_id='mytask', python_callable=_mytask).",
    ),
    f"C{BASE_ID}02": (
        "Avoid mixing task dependency directions",
        "mixed-dependency-directions",
        "For consistency don't mix directions in a single statement, instead split "
        "over multiple statements.",
    ),
    f"C{BASE_ID}03": (
        "TODO Task {} has no dependencies. Verify or disable message.",
        "task-no-dependencies",
        "TODO Sometimes a task without any dependency is desired, however often it is "
        "the result of a forgotten dependency.",
    ),
    f"C{BASE_ID}04": (
        "TODO Rename **kwargs variable to **context to show intent for Airflow task context",
        "task-context-argname",
        "TODO Indicate you expect Airflow task context variables in the **kwargs "
        "argument by renaming to **context.",
    ),
    f"C{BASE_ID}05": (
        "TODO Extract variables from keyword arguments for explicitness",
        "task-context-separate-arg",
        "TODO To avoid unpacking kwargs from the Airflow task context in a function, you "
        "can set the needed variables as arguments in the function.",
    ),
}


def collect_operators_from_binops(working_node: astroid.BinOp) -> Set[str]:
    """
    Function for collecting binary operations (>> and/or <<); called with recursion.
    """
    binops_found = set()
    if isinstance(working_node.left, astroid.BinOp):
        binops_found.update(collect_operators_from_binops(working_node.left))
    if isinstance(working_node.right, astroid.BinOp):
        binops_found.update(collect_operators_from_binops(working_node.right))
    if working_node.op in (">>", "<<"):
        binops_found.add(working_node.op)

    return binops_found


def is_assign_call_subtype_of_base_operator(node: astroid.Assign) -> bool:
    if not isinstance(node.value, astroid.Call):
        return False

    function_node = safe_infer(node.value.func)
    return (
        function_node
        and not isinstance(function_node, astroid.bases.BoundMethod)
        and hasattr(function_node, "is_subtype_of")
        and (
            function_node.is_subtype_of("airflow.models.BaseOperator")
            or function_node.is_subtype_of("airflow.models.baseoperator.BaseOperator")
            # ^ TODO: are both of these subtypes relevant?
        )
    )


class OperatorChecker(checkers.BaseChecker):
    """Checks on Airflow operators."""

    msgs = OPERATOR_CHECKER_MSGS

    @utils.only_required_for_messages("different-operator-varname-taskid", "match-callable-taskid")
    def visit_assign(self, node):
        """
        TODO rewrite this
        Check if operators using python_callable argument call a function with name
        '_[task_id]'. For example:
        Valid ->
        def _mytask(): print("dosomething")
        mytask = PythonOperator(task_id="mytask", python_callable=_mytask)

        Invalid ->
        def invalidname(): print("dosomething")
        mytask = PythonOperator(task_id="mytask", python_callable=invalidname)
        """
        # TODO: add check to force kwargs for task definitions

        if is_assign_call_subtype_of_base_operator(node):
            var_name = node.targets[0].name
            task_id = None
            python_callable_name = None

            for keyword in node.value.keywords:
                if keyword.arg == "task_id" and isinstance(keyword.value, astroid.Const):
                    # TODO support other values than constants
                    task_id = keyword.value.value
                    continue
                if keyword.arg == "python_callable":
                    python_callable_name = keyword.value.name

            self.check_operator_varname_versus_task_id(node, var_name, task_id)

            self.check_callable_name_versus_task_id(node, python_callable_name, task_id)

    def check_operator_varname_versus_task_id(
        self, node: astroid.Assign, var_name: str, task_id: str
    ) -> None:
        """Adds a message if the assigned variable name and the task ID do not match.
        A message is not added if either string argument is empty ("") or None."""
        if var_name and task_id and var_name != task_id:
            self.add_message("different-operator-varname-taskid", node=node)

    def check_callable_name_versus_task_id(
        self, node: astroid.Assign, python_callable_name: str, task_id: str
    ):
        """Adds a message if the callable name and the task ID prefixed with an underscore
        do not match. A message is not added if either string argument is empty ("") or None."""
        if python_callable_name and task_id and f"_{task_id}" != python_callable_name:
            self.add_message("match-callable-taskid", node=node)

    @utils.only_required_for_messages("mixed-dependency-directions")
    def visit_binop(self, node):
        """Check for mixed dependency directions."""

        self.check_mixed_dependency_directions(node)

    def check_mixed_dependency_directions(self, node: astroid.BinOp) -> None:
        """Check for mixed dependency directions (a BinOp chain contains both >> and <<)."""
        collected_operators = collect_operators_from_binops(node)
        if ">>" in collected_operators and "<<" in collected_operators:
            self.add_message("mixed-dependency-directions", node=node)
