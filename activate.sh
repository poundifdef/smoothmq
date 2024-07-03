
#!/bin/bash
set -e

# Get the directory of the current script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change to that directory
cd "$script_dir"

# Confirm the current directory
echo "Changed directory to: $(pwd)"
echo "activate.sh change working directory to $(pwd)"


if [ "$IN_ACTIVATED_ENV" = "1" ]; then
  IN_ACTIVATED_ENV=1
else
  IN_ACTIVATED_ENV=0
fi

# If the 'venv' directory doesn't exist, print a message and exit.
if [ ! -d "venv" ]; then
  cwd=$(pwd)
  echo "The 'venv' directory in $cwd does not exist, creating..."
  echo "OSTYPE: $OSTYPE"
  case "$OSTYPE" in
    darwin*|linux-gnu*)
      python3 ./install.py
      ;;
    *)
      python ./install.py
      ;;
  esac

  . ./venv/bin/activate
  export IN_ACTIVATED_ENV=1
  this_dir=$(pwd)
  export PATH="$this_dir:$PATH"
  echo "Environment created."
  pip install -r requirements.txt
  exit 0
fi

if [ "$IN_ACTIVATED_ENV" != "1" ]; then
  . ./venv/bin/activate
  export IN_ACTIVATED_ENV=1
fi
