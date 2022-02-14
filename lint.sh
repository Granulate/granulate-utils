#!/bin/bash
set -e

if [ -f venv/bin/activate ]; then
  source venv/bin/activate
fi

check_arg=""
if [[ "$1" = "--ci" ]]; then
    check_arg="--check"
fi

isort --settings-path .isort.cfg --skip granulate_utils/containers/cri/generated .
black --line-length 120 $check_arg --exclude granulate_utils/containers/cri/generated .
flake8 --config .flake8 .
mypy --exclude 'build|granulate_utils/containers/cri/generated' .
