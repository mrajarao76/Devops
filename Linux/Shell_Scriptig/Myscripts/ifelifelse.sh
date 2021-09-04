#!/bin/bash

MY_SHELL="csh"

if	[ "$MY_SHELL"="bash" ]
then
	echo  "you seem to like the bash shell"
elif	[ "$MY_SHELL" = "csh" ]
then
	echo "You seem to like the csh shell"
else
	echo "you don't seem to like the bash shell"
fi

