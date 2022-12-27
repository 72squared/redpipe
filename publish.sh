#!/usr/bin/env bash

repo="$1"

if [ -z "$repo" ]
then
    repo="pypi"
fi

make cleanall || exit 1

python3 setup.py sdist bdist_wheel || exit 1

twine upload -r "$repo" dist/* || exit 1

