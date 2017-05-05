#!/bin/bash
if [[ -z "$1" ]]
then
    docker-compose -f docker-compose.yml kill
        docker-compose -f docker-compose.yml up -d redis
else
    docker-compose -f docker-compose.yml "$@"
fi
