#!/usr/bin/env bash

ORIGINDIR=$( pwd )
BASEDIR=$( dirname $0 )
cd $BASEDIR

# set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
if [ -n "$HOP_JAVA_HOME" ]; then
  _HOP_JAVA=$HOP_JAVA_HOME/bin/java
elif [ -n "$JAVA_HOME" ]; then
  _HOP_JAVA=$JAVA_HOME/bin/java
else
  _HOP_JAVA="java"
fi

# Settings for all OSses
#
if [ -z "$HOP_OPTIONS" ]; then
  HOP_OPTIONS="-Xmx2048m"
fi
# optional line for attaching a debugger
#
#HOP_OPTIONS="${HOP_OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5007"

# Add HOP variables if they're set:
#
if [ -n "${HOP_AUDIT_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_AUDIT_FOLDER=${HOP_AUDIT_FOLDER}"
fi
if [ -n "${HOP_CONFIG_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_CONFIG_FOLDER=${HOP_CONFIG_FOLDER}"
fi
if [ -n "${DHOP_SHARED_JDBC_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_SHARED_JDBC_FOLDER=${HOP_SHARED_JDBC_FOLDER}"
fi

HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PLATFORM_RUNTIME=Server -DHOP_AUTO_CREATE_CONFIG=Y -DHOP_PLATFORM_OS="$(uname -s)

case $( uname -s ) in
	Linux) 
		CLASSPATH="lib/*:libswt/linux/$( uname -m )/*" 
		;;
	Darwin) 
		CLASSPATH="lib/*:libswt/osx64/*" 
		HOP_OPTIONS="${HOP_OPTIONS} -XstartOnFirstThread"
		;;
esac

if [ ! "x$JAAS_LOGIN_MODULE_CONFIG" = "x" -a ! "x$JAAS_LOGIN_MODULE_NAME" = "x" ]; then
	HOP_OPTIONS=$HOP_OPTIONS" -Djava.security.auth.login.config=$JAAS_LOGIN_MODULE_CONFIG"
	HOP_OPTIONS=$HOP_OPTIONS" -Dloginmodulename=$JAAS_LOGIN_MODULE_NAME"
fi

# OPTIONS="$OPTIONS -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

"$_HOP_JAVA" ${HOP_OPTIONS} -Djava.library.path=$LIBPATH  -classpath ${CLASSPATH} org.apache.hop.www.HopServer $@
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE

