
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

    