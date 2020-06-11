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

if [ -n "${HOP_AUDIT_DIRECTORY}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_AUDIT_DIRECTORY=${HOP_AUDIT_DIRECTORY}"
fi
if [ -n "${HOP_CONFIG_DIRECTORY}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_CONFIG_DIRECTORY=${HOP_CONFIG_DIRECTORY}"
fi
if [ -n "${HOP_SHARED_JDBC_DIRECTORY}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_SHARED_JDBC_DIRECTORY=${HOP_SHARED_JDBC_DIRECTORY}"
fi

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

