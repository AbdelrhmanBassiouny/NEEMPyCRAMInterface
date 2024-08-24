import importlib
import subprocess
import sys
import os
import rospkg
import rospy

PACKAGE_NAME = "pycram"
REQUIRED_COMMIT = "284a1b843b0e0f55ece06de68f4170b6ed44ca48"
CHECKED = False


def get_installed_commit_hash():
    try:
        # Get the package directory using rospkg
        package_dir = rospkg.RosPack().get_path(PACKAGE_NAME)

        # Use git to get the current commit hash
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=package_dir
        ).strip().decode('utf-8')

        return commit_hash

    except Exception as e:
        sys.exit(f"Failed to get commit hash: {e}")


def check_commit():
    commit_hash = get_installed_commit_hash()

    msg = f"Commit hash for {PACKAGE_NAME} does not match the required commit {REQUIRED_COMMIT},"
    f" please checkout the required commit before using this package, by running:\n"
    f"git checkout {REQUIRED_COMMIT} \n"
    f"in the {PACKAGE_NAME} package directory."

    if commit_hash != REQUIRED_COMMIT:
        rospy.logwarn(msg)


if not CHECKED:
    check_commit()
    CHECKED = True

from .neem_pycram_interface import *
