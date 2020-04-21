#!/usr/bin/env bash

ORIGINDIR=$( pwd )
BASEDIR=$( dirname $0 )
cd $BASEDIR

# Settings for all OSses
#
OPTIONS='-Xmx1g'

# optional line for attaching a debugger
#
OPTIONS="${OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"


case $( uname -s ) in
        Linux) 
                CLASSPATH="lib/*:libswt/linux/$( uname -m )/*" 
                ;;
        Darwin) 
                CLASSPATH="lib/*:libswt/osx64/*" 
                OPTIONS="${OPTIONS} -XstartOnFirstThread"
                ;;
esac


java ${OPTIONS} -classpath "${CLASSPATH}" org.apache.hop.cli.HopRun "$@"
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE

