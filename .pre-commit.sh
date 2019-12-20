#!/usr/bin/env bash

set -eo pipefail

isort -rc druzhba
isort -rc test

black druzhba
black test
