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
BASEDIR=$(dirname "$0")
cd "${BASEDIR}" || exit 1

# set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
if [ -n "${HOP_JAVA_HOME}" ]; then
  _HOP_JAVA="${HOP_JAVA_HOME}/bin/java"
elif [ -n "${JAVA_HOME}" ]; then
  _HOP_JAVA="${JAVA_HOME}/bin/java"
else
  _HOP_JAVA="java"
fi

# Settings for all OSses
#
if [ -z "${HOP_OPTIONS}" ]; then
  HOP_OPTIONS="-Xmx2048m"
fi

# optional line for attaching a debugger
#
if [ "$1" = "debug" ] || [ "$1" = "DEBUG" ]; then
  HOP_OPTIONS="${HOP_OPTIONS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
fi

# Add HOP variables if they're set:
#
if [ -n "${HOP_AUDIT_FOLDER}" ]; then
  HOP_OPTIONS="${HOP_OPTIONS} -DHOP_AUDIT_FOLDER=${HOP_AUDIT_FOLDER}"
else
  HOP_OPTIONS="${HOP_OPTIONS} -DHOP_AUDIT_FOLDER=./audit"
fi
if [ -n "${HOP_CONFIG_FOLDER}" ]; then
  HOP_OPTIONS="${HOP_OPTIONS} -DHOP_CONFIG_FOLDER=${HOP_CONFIG_FOLDER}"
fi
if [ -n "${HOP_SHARED_JDBC_FOLDERS}" ]; then
  HOP_OPTIONS="${HOP_OPTIONS} -DHOP_SHARED_JDBC_FOLDERS=${HOP_SHARED_JDBC_FOLDERS}"
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

HOP_OPTIONS="${HOP_OPTIONS} -DHOP_PLATFORM_RUNTIME=GUI -DHOP_AUTO_CREATE_CONFIG=Y -DHOP_PLATFORM_OS="$(uname -s)
HOP_OPTIONS="${HOP_OPTIONS} --add-opens java.xml/jdk.xml.internal=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.nio.cs=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"

os_arch="$(${_HOP_JAVA} -XshowSettings:properties -version 2>&1 | grep "os.arch" | awk -F= '{print $2}' | xargs)"
arch_path="$(echo "$os_arch" | sed 's/aarch/arm/g' | sed 's/amd/x86_/g')"
case $(uname -s) in
Linux)
  # Workaround for https://github.com/apache/hop/issues/4252
  # Related to https://github.com/eclipse-platform/eclipse.platform.swt/issues/639
  # And to some extent also https://github.com/eclipse-platform/eclipse.platform.swt/issues/790
  if [ "${XDG_SESSION_TYPE}" == "wayland" ]; then
    export GDK_BACKEND=x11
  fi
  os_path="linux"
  ;;
*BSD)
  os_path="unix"
  if [ "${XDG_SESSION_TYPE}" = "wayland" ]; then
    export GDK_BACKEND=x11
  fi
  if ! pkg info swt >/dev/null 2>&1; then
    echo "Install swt package..."
    pkg install -y swt >/dev/null
  fi
  swt_path="$ORIGINDIR/lib/swt/$os_path/$arch_path"
  if [ ! -f "$swt_path/swt.jar" ]; then
    [ ! -d "$swt_path" ] && mkdir -p "$swt_path"
    cp /usr/local/share/java/classes/swt.jar "$swt_path"
  fi
  lib_path="$HOME/.swt/lib/$(uname -s | tr '[:upper:]' '[:lower:]')/$os_arch"
  mkdir -p "$(dirname "$lib_path")"
  if [ ! -L "$lib_path" ]; then
    [ -d "$lib_path" ] && rm "$lib_path"
    ln -s /usr/local/lib "$lib_path"
  fi
  ;;
Darwin)
  os_path="osx"
  HOP_OPTIONS="${HOP_OPTIONS} -XstartOnFirstThread"
  ;;
esac
CLASSPATH="lib/core/*:lib/beam/*:lib/swt/$os_path/$arch_path/*"

"${_HOP_JAVA}" ${HOP_OPTIONS} -Djava.library.path="${LIBPATH}" -classpath "${CLASSPATH}" org.apache.hop.ui.hopgui.HopGui "$@"
EXITCODE=$?

cd "${ORIGINDIR}" || exit 1
exit $EXITCODE
