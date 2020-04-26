#!/usr/bin/env bash

ORIGINDIR=$( pwd )
BASEDIR=$( dirname $0 )
cd $BASEDIR

# set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
if [ -n "$HOP_JAVA_HOME" ]; then
  _HOP_JAVA=$HOP_JAVA_HOME
elif [ -n "$JAVA_HOME" ]; then
  _HOP_JAVA=$JAVA_HOME
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
HOP_OPTIONS="${HOP_OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"


case $( uname -s ) in
	Linux)
		CLASSPATH="lib/*:libswt/linux/$( uname -m )/*"
		;;
	Darwin)
		CLASSPATH="lib/*:libswt/osx64/*"
		OPTIONS="${OPTIONS} -XstartOnFirstThread"
		;;
esac

"$_HOP_JAVA" ${HOP_OPTIONS} -Djava.library.path=$LIBPATH -classpath "${CLASSPATH}" org.apache.hop.ui.hopgui.HopGui $@
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE
