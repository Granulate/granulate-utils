#!/bin/bash
set -xe

if [ -f venv/bin/activate ]; then
  source venv/bin/activate
elif [ -f .venv/bin/activate ]; then
  source .venv/bin/activate
fi

check_arg=""
if [[ "$1" = "--ci" ]]; then
    check_arg="--check"
fi

# see also isort --skip and flake8 config.
EXCLUDE_RE='.venv|venv|build|granulate_utils/generated'

python3 -m isort --settings-path .isort.cfg $check_arg --skip granulate_utils/generated --skip venv --skip .venv --skip build .
python3 -m black --check --diff --color --line-length 120 $check_arg --exclude $EXCLUDE_RE .
python3 -m flake8 --config .flake8  --exclude $(echo $EXCLUDE_RE | tr '|' ',') .
python3 -m mypy --exclude $EXCLUDE_RE .
