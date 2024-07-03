#!/bin/bash

# cd to script directory
cd "$(dirname "$0")"

# if venv doesn't exist, create it
if [ ! -d "venv" ]; then
    ./install
fi

# if windows use venv/Scripts/activate, else venv/bin/activate
if [[ "$OSTYPE" == "msys" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

