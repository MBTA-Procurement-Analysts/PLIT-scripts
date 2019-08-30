import pandas as pd
import numpy as np
from plitmongo.get_envs import get_env_vars
from plitmongo.configme import PANDAS_REPLACE_TABLE

def get_df_and_db(setname, queryname, datestring, basepath=""):
    env = get_env_vars()
    if basepath == "":
        basepath = env['RUBIXTAPEBASEPATH']
    df = _get_df("{0}/data/{1}/{2}/{3}-{1}.xlsx".format(basepath, setname, datestring, queryname))

    pass

def _get_df(path):
    raw_df = pd.read_excel(path, skip = 1)
    for old, new in PANDAS_REPLACE_TABLE:
        raw_df.columns = [c.replace(old, new) for c in raw_df.columns]
    return raw_df

def _get_db(dbtype):
    pass
