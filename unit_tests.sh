#!/bin/sh
#
# Use this script to run unit tests.


set -e # Exit early if any commands fail


# - Edit this to change how unit tests run locally
exec python3 -m unittest discover -s tests/unit