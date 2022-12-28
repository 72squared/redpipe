#!/usr/bin/env bash

command -v virtualenv >/dev/null 2>&1 || { echo >&2 "I require virtualenv but it's not installed.  Aborting."; exit 1; }

root_dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

venv_dir="$root_dir/.venv"


if [ ! -f "$venv_dir/bin/python" ]
then
    echo "configuring virtualenv $venv_dir ..."
    virtualenv -q "$venv_dir" || { echo >&2 "unable to configure the virtualenv for the project in $venv_dir"; exit 1; }
fi

"$venv_dir/bin/python" -m pip install --upgrade pip
if [ $? -ne 0 ]
then
    >&2 echo "failed to upgrade pip"
    exit 1
fi

"$venv_dir/bin/pip" install -q -r "$root_dir/dev-requirements.txt"

if [ $? -ne 0 ]
then
    >&2 echo "failed to install pip packages"
    exit 1
fi

