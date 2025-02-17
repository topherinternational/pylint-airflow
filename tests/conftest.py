"""Globally accessible helpers functions/classes in all tests."""

import os

import pytest
from pylint.testutils import (
    LintModuleTest,
    FunctionalTestFile,
)

pytest_plugins = ["helpers_namespace"]


class PylintAirflowLintModuleTest(LintModuleTest):
    """
    Implemented this class because I didn't want tests in the pylint-airflow package itself. Had to
    do some yak-shaving to get it to work.

    Picked the useful parts from Pylint, and inspired by
    https://github.com/PyCQA/pylint-django/blob/2.0.5/pylint_django/tests/test_func.py#L18-L24

    With this class, you can now simply pass a file path, and run the test.
    Messages can be ignored in the file itself with '# [symbol]', e.g.:

    foobar = Magic()  # [no-magic]

    Defining expected messages in a .txt is not supported with this.
    And ignore messages in the file itself with # pylint:disable=[symbol]
    """

    def __init__(self, test_filepath):
        test_dirname = os.path.dirname(test_filepath)
        test_basename = os.path.basename(test_filepath)
        func_test = FunctionalTestFile(directory=test_dirname, filename=test_basename)
        super().__init__(func_test)

        self._test_filepath = test_filepath
        self._linter.load_plugin_modules(["pylint_airflow"])

    def _get_expected_messages(self):
        with self._open_source_file() as test_file:
            return self.get_expected_messages(test_file)

    def check_file(self):
        """Run Pylint on a file."""
        self._linter.check((self._test_filepath,))

        expected_msgs = self._get_expected_messages()
        received_msgs, received_text = self._get_actual()
        linesymbol_text = {(ol.lineno, ol.symbol): ol.msg for ol in received_text}

        if expected_msgs != received_msgs:
            missing, unexpected = self.multiset_difference(expected_msgs, received_msgs)
            msg = [f"Wrong results for file '{self._test_file.base}':"]
            if missing:
                msg.append("\nExpected in testdata:")
                msg.extend(symbol for _, symbol in sorted(missing))
            if unexpected:
                msg.append("\nUnexpected in testdata:")
                msg.extend(
                    f" {line_nr:3d}: {symbol} - {linesymbol_text[(line_nr, symbol)]}"
                    for line_nr, symbol in sorted(unexpected)
                )
            pytest.fail("\n".join(msg))


@pytest.helpers.register
def functional_test(filepath):
    """Run Pylint on a file, given the path to the file."""
    lint_test = PylintAirflowLintModuleTest(filepath)
    lint_test.check_file()


@pytest.helpers.register
def file_abspath(file):
    """Fetch the absolute path to the directory of a file."""
    return os.path.abspath(os.path.dirname(file))
