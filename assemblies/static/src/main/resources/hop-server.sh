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

if [ ! "x$JAAS_LOGIN_MODULE_CONFIG" = "x" -a ! "x$JAAS_LOGIN_MODULE_NAME" = "x" ]; then
	OPTIONS=$OPTIONS" -Djava.security.auth.login.config=$JAAS_LOGIN_MODULE_CONFIG"
	OPTIONS=$OPTIONS" -Dloginmodulename=$JAAS_LOGIN_MODULE_NAME"
fi

OPTIONS="$OPTIONS -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

java ${OPTIONS} -classpath ${CLASSPATH} org.apache.hop.www.HopServer $@
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE

