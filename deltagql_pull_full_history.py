import deltaGQL
import sys
import subprocess

# Get required parameters
querybasename = sys.argv[1]
quantity = sys.argv[2]
unit = deltaGQL.check_argument(3)

# Run tape files in loop
deltaGQL.pull_historical_data(querybasename, quantity, unit)

