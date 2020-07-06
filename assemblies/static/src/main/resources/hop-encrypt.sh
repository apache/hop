#!/usr/bin/env bash

ORIGINDIR=$( pwd )
BASEDIR=$( dirname $0 )
cd $BASEDIR

# Settings for all OSses
#
OPTIONS='-Xmx64m'

# optional line for attaching a debugger
#
# OPTIONS="${OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5009"

if [ -n "${HOP_AUDIT_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_AUDIT_FOLDER=${HOP_AUDIT_FOLDER}"
fi
if [ -n "${HOP_CONFIG_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_CONFIG_FOLDER=${HOP_CONFIG_FOLDER}"
fi
if [ -n "${DHOP_SHARED_JDBC_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_SHARED_JDBC_FOLDER=${HOP_SHARED_JDBC_FOLDER}"
fi

HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PLATFORM_RUNTIME=Encrypt -DHOP_PLATFORM_OS="$(uname -s)

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

