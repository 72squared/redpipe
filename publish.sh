#!/usr/bin/env bash

repo="$1"

if [ -z "$repo" ]
then
    repo="pypi"
fi

python setup.py register -r "$repo" && \
    python setup.py sdist upload -r "$repo"

