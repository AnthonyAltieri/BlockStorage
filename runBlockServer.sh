#!/bin/bash

# This script is meant to start your metadata server

# Check the parameters provided
if [ "$#" -ne 1 ]
then
	echo "Wrong arguments, usage :"
	echo "./runBlockServer.sh <config_file>"
	exit
fi


CONFIGFILE_PATH=$1

python BlockServer.py $CONFIGFILE_PATH
