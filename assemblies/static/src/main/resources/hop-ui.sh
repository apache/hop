#!/usr/bin/env bash

ORIGINDIR=$( pwd )
BASEDIR=$( dirname $0 )
cd $BASEDIR

# Settings for all OSses
#
OPTIONS='-Xmx1g'

case $( uname -s ) in
	Linux) 
		CLASSPATH="lib/*:libswt/linux/$( uname -m )/*" 
		;;
	Darwin) 
		CLASSPATH="lib/*:libswt/osx64/*" 
		OPTIONS="${OPTIONS} -XstartOnFirstThread"
		;;
esac

java ${OPTIONS} -classpath ${CLASSPATH} org.apache.hop.ui.hopui.HopUi $@
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE

