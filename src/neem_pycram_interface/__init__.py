import subprocess
import sys
import os
import rospkg
import rospy

PACKAGE_NAME = "pycram"
REQUIRED_COMMIT = "056fddcb852aac25983f2084dc045e0b9e996772"
REQUIRED_BRANCH = "master"
REQUIRED_REMOTE = "git@github.com:AbdelrhmanBassiouny/pycram.git"
CHECKED = False


def get_installed_commit_hash_and_branch():
    try:
        # Get the package directory using rospkg
        package_dir = rospkg.RosPack().get_path(PACKAGE_NAME)

        # Use git to get the current commit hash
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=package_dir
        ).strip().decode('utf-8')

        # get the current branch
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=package_dir
        ).strip().decode('utf-8')

        # get the current branch url
        remote = subprocess.check_output(
            ["git", "config", "--get", f"remote.origin.url"],
            cwd=package_dir
        ).strip().decode('utf-8')

        return commit_hash, branch, remote

    except Exception as e:
        sys.exit(f"Failed to get commit hash and branch: {e}")


def check_commit():
    commit_hash, branch, remote = get_installed_commit_hash_and_branch()

    msg = (f"Commit hash {commit_hash} of branch {branch} of remote {remote} for {PACKAGE_NAME} does not match the"
           f" required commit {REQUIRED_COMMIT} of branch {REQUIRED_BRANCH} or remote {REQUIRED_REMOTE}. "
           f"please checkout the required commit before using this package, by running the following command in the"
           f" {PACKAGE_NAME} package directory:\n"
           f"git checkout {REQUIRED_COMMIT} \n")
    if commit_hash != REQUIRED_COMMIT:
        rospy.logwarn(msg)


if not CHECKED:
    check_commit()
    CHECKED = True

from .neem_pycram_interface import *
