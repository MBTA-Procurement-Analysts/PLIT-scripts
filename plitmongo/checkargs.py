from datetime import datetime
from plitmongo.configme import MONGO_POSSIBLE_DBTYPES


def parseArgs(args):
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
        raise ValueError(
            "Date String {} is not in the correct format (mmddyyyy-hhmmss).".format(datestring))

    if dbtype not in MONGO_POSSIBLE_DBTYPES:
        raise ValueError("Database Type String {} not valid. Should be one of {}.".format(
            dbtype, MONGO_POSSIBLE_DBTYPES))

    return (datestring, dbtype)
