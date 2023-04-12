#! /bin/bash

cd /opt/dagster/app
pip install -e '.[dev]'

dagit -h 0.0.0.0 -p 3000 -w ./workspace.yaml