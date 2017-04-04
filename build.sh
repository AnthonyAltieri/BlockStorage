#!/bin/bash

# Check the parameters provided
if [ "$#" -ne 1 ]
then
	echo "Wrong arguments, usage :"
	echo "./build.sh <build or clean>"
	exit
fi

BUILD_OR_CLEAN=$1

if [ $BUILD_OR_CLEAN == "build" ]
then
	echo "Building"
	# Enter your build code here, think about make
	echo "Generating thrift for metadataServer.thrift"
    thrift --gen py -r metadataServer.thrift
	echo "Generating thrift for blockServer.thrift"
    thrift --gen py -r blockServer.thrift

	# Fill in the build part, see comments below

	exit

elif [ $BUILD_OR_CLEAN == "clean" ]
then
	echo "Cleaning"
	# Do the cleaning part here, think of make clean
    rm -rf gen-py

	# Fill in code specific cleanup similar to what you would do in make clean
else
	echo "Wrong build command"
	echo "Either ./build.sh build or ./build.sh clean"
	exit
fi
# This script is used to build your code

# Since many of you will use different programming languages, you can call your
# build file from here

# If you are using an interpreted language like python, just leave as it is

# Eg: for CPP
#
# SRC_DIR = src/
# INC_DIR = inc/
# etc.
# g++ ...
# If you want, you may call a make file from here
# make

# For Java
# Call ant

# Autograder will be calling this script to build your source code
