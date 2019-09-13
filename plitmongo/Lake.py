import pandas as pd
import numpy as np
from pymongo import MongoClient
import os
from datetime import datetime
from plitmongo.configme import PANDAS_REPLACE_TABLE
from plitmongo.configme import ENVS_TO_GET
from plitmongo.configme import MONGO_ALL_DBTYPES
from plitmongo.configme import MONGO_POSSIBLE_DBTYPES

class Lake:

    def __init__(self):
        self.env = self._get_env_vars()

    def get_df(self, setname, queryname, datestring, basepath=""):
        print("---- Getting Query File ----")
        if basepath == "":
            print("Basepath not specified. Reading from Env. Variables.")
            basepath = self.env['RUBIXTAPEBASEPATH']
        print("Date String: {}".format(datestring))
        print("Set Name: {}".format(setname))
        print("Query: {}".format(queryname))
        print("-"*28)
        return self._get_df_raw_path("{0}/data/{1}/{2}/{3}-{2}.xlsx".format(basepath, setname, datestring, queryname))

    def _get_env_vars(self):
        res = {}
        for env in ENVS_TO_GET:
            if env in os.environ:
                res[env] = os.environ[env]
            else:
                raise EnvironmentError("Environment Variable {} is not found on system.".format(env))
        return res

    def _get_df_raw_path(self, path):
        raw_df = pd.read_excel(path, skiprows = 1)
        for old, new in PANDAS_REPLACE_TABLE:
            raw_df.columns = [c.replace(old, new) for c in raw_df.columns]
        self._print_colnames(raw_df.columns)
        return raw_df

    def get_db(self, use_auth = True):
        auth_string = ""
        if use_auth:
            auth_string = "{}:{}@".format(self.env["RUBIXMONGOUSERNAME"], self.env["RUBIXMONGOPASSWORD"])
        connection_string = "mongodb://{}localhost:27017".format(auth_string)
        return MongoClient(connection_string)

    def get_db_names(self, dbtype):
        if dbtype == "both":
            return ["rubix-{}-{}".format(self.env["RUBIXLOCATION"], type_) for type_ in MONGO_ALL_DBTYPES]
        else:
            return ["rubix-{}-{}".format(self.env["RUBIXLOCATION"], dbtype)]

    def _print_colnames(self, colnames_arr):
        print("---- Column Names of Dataframe ----")
        for colname in colnames_arr:
            print(colname)
        print("-----------------------------------\n")

    @classmethod
    def parse_args(cls, args):
        """Validates and parses arguments passed onto the mongo script.

        Args:
            args ([]str): List of Arguments

        Returns:
            (str, str): Tuple of Date string for pathnames, and Destination database name
            str: 

        Raises:
            ValueError: If date string is invalid, or destination database name is unexpected.
        """
        error_string = """
            Arguments given unexpected: {}.\n
            Arguments Needed: Date String (mmddyyyy-hhmmss), and destination database: One of (dev, prod, both).\n
            """.format(args)

        if len(args) != 2:
            raise ValueError(error_string)

        datestring, dbtype = args

        try:
            datetime.strptime(datestring, "%m%d%Y-%H%M%S")
        except ValueError:
            raise ValueError("Date String {} is not in the correct format (mmddyyyy-hhmmss).".format(datestring))

        if dbtype not in MONGO_POSSIBLE_DBTYPES:
            raise ValueError("Database Type String {} not valid. Should be one of {}.".format(dbtype, MONGO_POSSIBLE_DBTYPES))
        
        return (datestring, dbtype)