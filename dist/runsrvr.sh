#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

#TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
#export ZKSERVER=lab1.cs.mcgill.ca:21803,lab2.cs.mcgill.ca:21803,lab3.cs.mcgill.ca:21803
export ZKSERVER=lab2-30.cs.mcgill.ca:21803,lab2-31.cs.mcgill.ca:21803,lab2-32.cs.mcgill.ca:21803


java -cp $CLASSPATH:../task:.: DistProcess 
