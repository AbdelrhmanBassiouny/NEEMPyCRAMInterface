import subprocess
import sys
import os
import rospkg
import rospy

PACKAGE_NAME = "pycram"
REQUIRED_COMMIT = "75f10fc2a8b43741108243617d57285eecb3c891"
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

    msg = (f"Commit hash {commit_hash} for {PACKAGE_NAME} does not match the required commit {REQUIRED_COMMIT}, "
           f"please checkout the required commit before using this package, by running the following command in the"
           f" {PACKAGE_NAME} package directory:\n"
           f"git checkout {REQUIRED_COMMIT} \n")

    if commit_hash != REQUIRED_COMMIT:
        rospy.logwarn(msg)


if not CHECKED:
    check_commit()
    CHECKED = True

from .neem_pycram_interface import *
