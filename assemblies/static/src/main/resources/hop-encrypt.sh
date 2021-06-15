#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

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
if [ -n "${HOP_SHARED_JDBC_FOLDER}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_SHARED_JDBC_FOLDER=${HOP_SHARED_JDBC_FOLDER}"
fi
if [ -n "${HOP_PLUGIN_BASE_FOLDERS}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PLUGIN_BASE_FOLDERS=${HOP_PLUGIN_BASE_FOLDERS}"
fi
if [ -n "${HOP_PASSWORD_ENCODER_PLUGIN}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PASSWORD_ENCODER_PLUGIN=${HOP_PASSWORD_ENCODER_PLUGIN}"
fi
if [ -n "${HOP_AES_ENCODER_KEY}" ]; then
    HOP_OPTIONS="${HOP_OPTIONS} -DHOP_AES_ENCODER_KEY=${HOP_AES_ENCODER_KEY}"
fi

HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PLATFORM_RUNTIME=Encrypt -DHOP_PLATFORM_OS="$(uname -s)

case $( uname -s ) in
	Linux) 
		CLASSPATH="lib/*"
		;;
	Darwin) 
		CLASSPATH="lib/*"
		HOP_OPTIONS="${HOP_OPTIONS} -XstartOnFirstThread"
		;;
esac

java ${HOP_OPTIONS} -classpath "${CLASSPATH}" org.apache.hop.encryption.HopEncrypt $@
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE

