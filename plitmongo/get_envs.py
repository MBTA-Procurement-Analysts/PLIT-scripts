"""plitmongo package-level environmental variables.

This file contains configutation variables to be shared acress the package
that is sensitive. This file can be committed to version control, since all
configuration variables are read using environmental variables of the host OS.

To temporarily use another configuration, use the shell prompt to assign the
desired variable before running the scripts.
"""
import os
from plitmongo.configme import ENVS_TO_GET

def get_env_vars():
    res = {}
    for env in ENVS_TO_GET:
        if env in os.environ:
            res[env] = os.environ[env]
        else:
            raise EnvironmentError("Environment Variable {} is not found on system.".format(env))
    return res