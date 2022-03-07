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

ORIGINDIR=$(pwd)
BASEDIR=$(dirname $0)
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

HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PLATFORM_RUNTIME=Server -DHOP_AUTO_CREATE_CONFIG=Y -DHOP_PLATFORM_OS="$(uname -s)

case $(uname -s) in
Linux)
    if $($_HOP_JAVA -XshowSettings:properties -version 2>&1| grep -q "os.arch = aarch64"); then
        CLASSPATH="lib/*:libswt/linux/arm64/*"
    else
        CLASSPATH="lib/*:libswt/linux/$(uname -m)/*"
    fi
  ;;
Darwin)
  if $($_HOP_JAVA -XshowSettings:properties -version 2>&1| grep -q "os.arch = aarch64"); then
      CLASSPATH="lib/*:libswt/osx/arm64/*"
  else
      CLASSPATH="lib/*:libswt/osx/x86_64/*"
  fi
  HOP_OPTIONS="${HOP_OPTIONS} -XstartOnFirstThread"
  ;;
esac

if [ ! "x$JAAS_LOGIN_MODULE_CONFIG" = "x" -a ! "x$JAAS_LOGIN_MODULE_NAME" = "x" ]; then
  HOP_OPTIONS=$HOP_OPTIONS" -Djava.security.auth.login.config=$JAAS_LOGIN_MODULE_CONFIG"
  HOP_OPTIONS=$HOP_OPTIONS" -Dloginmodulename=$JAAS_LOGIN_MODULE_NAME"
fi

"$_HOP_JAVA" ${HOP_OPTIONS} -Djava.library.path=$LIBPATH -classpath "${CLASSPATH}" org.apache.hop.www.HopServer "$@"
EXITCODE=$?

cd ${ORIGINDIR}
exit $EXITCODE
