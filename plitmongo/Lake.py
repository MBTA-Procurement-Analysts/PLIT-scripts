import os
import sys
import logging
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from plitmongo.configme import PANDAS_REPLACE_TABLE
from plitmongo.configme import ENVS_TO_GET
from plitmongo.configme import MONGO_ALL_DBTYPES
from plitmongo.configme import MONGO_POSSIBLE_DBTYPES


class Lake:

    def __init__(self):
        self._logger_setup()
        self.env = self._get_env_vars()
        self.is_cron = not os.isatty(sys.stdin.fileno())
        self._log("This script is {0}running from cron/jupyter.".format(
            "" if self.is_cron else "not "))

    def _logger_setup(self):
        _log_stream_handler = logging.StreamHandler()
        _log_stream_handler.setLevel(logging.INFO)
        _log_stream_handler.setFormatter(
            logging.Formatter("{asctime} {levelname}: {message} ", style="{"))
        self._logger = logging.getLogger("PLITmongo")
        self._logger.setLevel(logging.INFO)
        self._logger.addHandler(_log_stream_handler)

    def _log(self, msg):
        self._logger.info(msg)

    def _warn(self, msg):
        self._logger.warn(msg)

    def get_df_by_direct_path(self, path):
        return self._get_df_raw_path(path)

    def get_df(self, setname, queryname, datestring, basepath=""):
        self._log("---- Getting Query File ----")
        if basepath == "":
            self._log("Basepath not specified. Reading from Env. Variables.")
            basepath = self.env['RUBIXTAPEBASEPATH']
        self._log("Date String: {}".format(datestring))
        self._log("Set Name: {}".format(setname))
        self._log("Query: {}".format(queryname))
        self._log("-"*28)
        return self._get_df_raw_path("{0}/data/{1}/{2}/{3}-{2}.xlsx".format(basepath, setname, datestring, queryname))

    def _get_env_vars(self):
        res = {}
        for env in ENVS_TO_GET:
            if env in os.environ:
                res[env] = os.environ[env]
            else:
                raise EnvironmentError(
                    "Environment Variable {} is not found on system.".format(env))
        return res

    def _get_df_raw_path(self, path):
        raw_df = pd.read_excel(path, skiprows=1)
        for old, new in PANDAS_REPLACE_TABLE:
            raw_df.columns = [c.replace(old, new) for c in raw_df.columns]
        self._log("Dataframe imported and column names replaced.")
        self._log("Size of Dataframe is {}.".format(len(raw_df)))
        self._log_colnames(raw_df.columns)
        return raw_df

    def get_db(self, use_auth=True):
        auth_string = ""
        if use_auth:
            auth_string = "{}:{}@".format(
                self.env["RUBIXMONGOUSERNAME"], self.env["RUBIXMONGOPASSWORD"])
        connection_string = "mongodb://{}localhost:27017".format(auth_string)
        self.mongo_client = MongoClient(connection_string)
        return self.mongo_client

    def get_db_names(self, dbtype):
        if dbtype == "both":
            return ["rubix-{}-{}".format(self.env["RUBIXLOCATION"], type_) for type_ in MONGO_ALL_DBTYPES]
        else:
            return ["rubix-{}-{}".format(self.env["RUBIXLOCATION"], dbtype)]

    def _log_colnames(self, colnames_arr):
        self._log("---- Column Names of Dataframe ----")
        for colname in colnames_arr:
            self._log(colname)
        self._log("-----------------------------------")

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

        if dbtype not in MONGO_POSSIBLE_DBTYPES:
            raise ValueError("Database Type String {} not valid. Should be one of {}.".format(
                dbtype, MONGO_POSSIBLE_DBTYPES))

        return (datestring, dbtype)

    def end(self):
        self._log("Import loop complete. Closing mongoDB connection and exiting script.")
        self.mongo_client.close()