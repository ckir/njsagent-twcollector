#!/bin/bash
# fail fast
set -e
set -o pipefail

wget https://raw.githubusercontent.com/ckir/njsagent-twcollector/master/package.json
npm install
