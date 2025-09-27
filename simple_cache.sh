#!/bin/sh
#
# Use this script to run your program LOCALLY.

set -e # Exit early if any commands fail

# - Edit this to change how your program runs locally
python3 -m app.main "$@"
