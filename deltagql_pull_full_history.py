import deltaGQL
import sys
import subprocess

querybasename = sys.argv[1]
window = sys.argv[2]
unit = None
unit = deltaGQL.check_argument(3)

deltaGQL.pull_historical_data(querybasename, window, unit)

