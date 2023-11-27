#!/usr/bin/env sh

if [ -d $1 -a ! -z "$(ls -A $1)" ]
then
	printf "$1 already exists and contains the following files: $(ls -A -m -w 0 $1)\nAre you sure (y/n)? "
	read enter
	if [ $enter = 'y' ]
	then
		exit 0
	else
    echo "Understood. Exiting with error code."
		exit 1
	fi
fi
