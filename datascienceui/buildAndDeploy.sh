#!/bin/bash

docker build -t cictortc/cogClaimDataUI:latest .
docker push registry.ng.bluemix.net/cictortc/cogClaimDataUI
