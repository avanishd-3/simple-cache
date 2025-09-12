#!/bin/sh
#
# Use this script to run unit tests.


set -e # Exit early if any commands fail


# Edit to change how tests run locally
# Use -u flag to run unit tests only
# Use -i flag to run integration tests only
# If no flags are provided, both unit and integration tests will run

while getopts "uih" option; do
  case $option in
    u)
        python3 -m unittest discover -s tests/unit
      ;;
    i)
        python3 -m unittest discover -s tests/integration
      ;;
    h)
      echo "Usage: $0 [-u] [-i]"
      echo "  -u    Run unit tests only"
      echo "  -i    Run integration tests only"
      exit 0
      ;;
    *)
      # code to execute when an unknown flag is provided
      echo "Select test type: -u for unit tests, -i for integration tests"
      exit 1
      ;;
  esac
done

# If no flags are provided, run both unit and integration tests
if [ $OPTIND -eq 1 ]; then
    echo "Running unit tests";
    echo "----------------------------------------------------------------------";
    python3 -m unittest discover -s tests/unit
    echo "----------------------------------------------------------------------";
    echo "Running integration tests";
    python3 -m unittest discover -s tests/integration
fi

