#!/usr/bin/env bash

#gunicorn flaskproj:app
mkdir .log 2> /dev/null
DEBUG=0 gunicorn -b 127.0.0.1:5000 flaskproj:app --workers 4 --timeout 120 --access-logfile .log/access.log --error-logfile .log/general.log
