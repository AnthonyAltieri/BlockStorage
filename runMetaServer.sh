#!/bin/bash

# This script is meant to start your metadata server

# Check the parameters provided
if [ "$#" -ne 2 ]
then
	echo "Wrong arguments, usage :"
	echo "./runMetaServer.sh <config_file> <id>"
	exit
fi

# First parameter is the path for the config_file while second one is the ID
# ID field says whether its metadata server 1, 2 or n
CONFIGFILE_PATH=$1
ID=$2

python MetaServer.py $CONFIGFILE_PATH $ID
