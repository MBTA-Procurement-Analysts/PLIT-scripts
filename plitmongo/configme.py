"""plitmongo package-level configuations.

This file contains configutation variables to be shared acress the package
that is not sensitive.
"""

MONGO_PREFIX = 'rubix'
MONGO_POSSIBLE_LOCATIONS = ('local', 'ohio', 'both')
MONGO_POSSIBLE_DBTYPES = ('dev', 'prod', 'both')

PANDAS_REPLACE_TABLE = [(" ", "_"),
                        ("/", "_"),
                        (".", ""),
                        ("?", "")]

ENVS_TO_GET = ['RUBIXMONGOUSERNAME', 'RUBIXMONGOPASSWORD', 'RUBIXLOCATION', "RUBIXTAPEBASEPATH"]