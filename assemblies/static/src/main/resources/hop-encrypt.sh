#!/usr/bin/env bash

ORIGINDIR=$( pwd )
BASEDIR=$( dirname $0 )
cd $BASEDIR

# Settings for all OSses
#
OPTIONS='-Xmx64m'

# optional line for attaching a debugger
#
# OPTIONS="${OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"


case $( uname -s ) in
	Linux) 
		CLASSPATH="lib/*"
		;;
	Darwin) 
		CLASSPATH="lib/*"
		OPTIONS="${OPTIONS} -XstartOnFirstThread"
		;;
esac


java ${OPTIONS} -classpath "${CLASSPATH}" org.apache.hop.core.encryption.Encr $@
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE

