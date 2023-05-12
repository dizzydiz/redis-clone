#!/bin/sh
#
# DON'T EDIT THIS!
#
# CodeCrafters uses this file to test your code. Don't make any changes here!
#
# DON'T EDIT THIS!
set -e
# tmpFile=$(mktemp)
tmpFile="build/bin"
go build -o "$tmpFile" cmd/*.go
exec "$tmpFile"
