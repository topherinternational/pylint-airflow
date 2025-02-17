"""
This script is used by the CI to:
1. Validate if message ids are defined correctly (e.g. missed comma might evaluate correctly, but
   will be interpreted incorrectly).
2. For each message type, check if codes start at 0 and increment by 1, e.g. C8300, C8301, ...
"""
import re
from collections import defaultdict
from pathlib import Path
from typing import List

from pylint.lint import PyLinter

from pylint_airflow.__pkginfo__ import BASE_ID
from pylint_airflow.checkers import register_checkers


def has_pylint_airflow_base_id(msg_id: str):
    """Check if the message ID contains the expected BASE_ID infix"""
    return re.compile(f"[A-Z]{BASE_ID}[0-9]+").match(msg_id)


def check_if_msg_ids_increment(message_ids: List[str]):
    """
    Check if the message IDs (within 1 group) start at 0 and increment by 1. E.g. C8300, C8301, ...
    :param List[str] message_ids: Message IDs within single group
    """

    # Fetch only last 2 characters of the id
    ids = sorted([int(msg_id[3:5]) for msg_id in message_ids])

    # ids should start at 0, should be sorted, and should increment by 1.
    # So with e.g. 5 ids, check if the last id is 4.
    maxid = len(ids) - 1
    if ids[-1] != maxid:
        # Could come up with some sorting function for message_ids, but sorting numerically and
        # checking last id proved easier.
        formatted_message_ids = [f"{msg_type}{BASE_ID}{str(id_).zfill(2)}" for id_ in ids]
        raise AssertionError(f"Message ids should increment by 1. {formatted_message_ids}")


def check_if_msg_ids_in_readme(message_ids: List[str]):
    """
    Check if message IDs are listed in the README.
    :param List[str] message_ids: All message IDs found in pylint-airflow
    """

    readme_path = Path(__file__).resolve().parent.parent / "README.rst"
    with open(readme_path, mode="r", encoding="utf-8") as readme_file:
        readme_text = readme_file.read()

    not_found = [msg_id for msg_id in message_ids if msg_id not in readme_text]
    if not_found:
        raise AssertionError(
            f"Message IDs {not_found} not found in README. All message IDs should be documented."
        )


# Construct dict of {message type: [message ids]}
messages = defaultdict(list)
linter = PyLinter()
register_checkers(linter)
# Running register_checkers automatically validates there are no duplicate message ids
for message in linter.msgs_store.messages:
    if has_pylint_airflow_base_id(message.msgid):
        msg_type = message.msgid[0]
        messages[msg_type].append(message.msgid)

for msg_type, msg_ids in messages.items():
    check_if_msg_ids_increment(msg_ids)

check_if_msg_ids_in_readme([msg_id for msg_list in messages.values() for msg_id in msg_list])
