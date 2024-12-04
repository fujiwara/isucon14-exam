#!/bin/bash

cat /var/log/nginx/access.log | \
    perl -pE 's{/rides/[0-9A-Z]+/}{/rides/***/}' | \
    alp json -o count,method,uri,min,avg,max,sum,2xx,3xx,4xx,5xx -r
