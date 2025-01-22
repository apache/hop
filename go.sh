#!/bin/bash

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

export HOP_PATH=/Users/sergio/Serasoft/code/xdelox/hop
# export HOP_DEPLOY=/Users/sergio/Serasoft/hops/2.12.0-SNAPSHOT/hop
export HOP_DEPLOY=/Users/sergio/Serasoft/code/xdelox/hop/assemblies/client/target/hop

# Check if an argument was provided
if [ -n "$1" ] && [ "$1" = "rebuild" ]; then
  echo "Building..."
  mvn clean install -pl org.apache.hop:hop-core -DskipTests
  mvn clean install  -pl org.apache.hop:hop-ui -DskipTests
  mvn clean install  -pl org.apache.hop:hop-engine -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-textfile -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-excel -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-selectvalues -DskipTests
 # mvn package -o -pl org.apache.hop:hop-transform-tableinput -DskipTests
  mvn clean install -o -pl org.apache.hop:hop-transform-tableinput -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-misc-static-schema -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-javascript -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-script -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-databasejoin -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-transform-sql -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-action-sql -DskipTests
#  mvn clean install -o -pl org.apache.hop:hop-action-waitforsql -DskipTests
  echo $HOP_PATH
  echo $HOP_DEPLOY

 mv $HOP_PATH/core/target/hop-core-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
 mv $HOP_PATH/ui/target/hop-ui-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
 mv $HOP_PATH/engine/target/hop-engine-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
# mv $HOP_PATH/plugins/transforms/textfile/target/hop-transform-textfile-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
# mv $HOP_PATH/plugins/transforms/textfile/target/hop-transform-excel-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
 # mv $HOP_PATH/plugins/misc/static-schema/target/hop-misc-static-schema-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
# mv $HOP_PATH/plugins/transforms/selectvalues/target/hop-transform-selectvalues-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
mv $HOP_PATH/plugins/transforms/tableinput/target/hop-transform-tableinput-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
#  mv $HOP_PATH/plugins/transforms/janino/target/hop-transform-janino-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
#  mv $HOP_PATH/plugins/transforms/javascript/target/hop-transform-javascript-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
#  mv $HOP_PATH/plugins/transforms/sql/target/hop-transform-sql-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
#  mv $HOP_PATH/plugins/actions/sql/target/hop-action-sql-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
#  mv $HOP_PATH/plugins/actions/sql/target/hop-action-waitforsql-2.12.0-SNAPSHOT.zip $HOP_DEPLOY
  cd $HOP_DEPLOY
  unzip -o hop-core-2.12.0-SNAPSHOT.zip
  unzip -o hop-ui-2.12.0-SNAPSHOT.zip
  unzip -o hop-engine-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-textfile-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-excel-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-selectvalues-2.12.0-SNAPSHOT.zip
  unzip -o hop-transform-tableinput-2.12.0-SNAPSHOT.zip
#  unzip -o hop-misc-static-schema-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-janino-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-javascript-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-script-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-databasejoin-2.12.0-SNAPSHOT.zip
#  unzip -o hop-transform-sql-2.12.0-SNAPSHOT.zip
#  unzip -o hop-action-sql-2.12.0-SNAPSHOT.zip
#  unzip -o hop-action-waitforsql-2.12.0-SNAPSHOT.zip

#  rm hop-transform-selectvalues-2.12.0-SNAPSHOT.zip
  rm hop-transform-tableinput-2.12.0-SNAPSHOT.zip
  rm hop-core-2.12.0-SNAPSHOT.zip
  rm hop-ui-2.12.0-SNAPSHOT.zip
  rm hop-engine-2.12.0-SNAPSHOT.zip
#  rm hop-transform-textfile-2.12.0-SNAPSHOT.zip
#  rm hop-transform-excel-2.12.0-SNAPSHOT.zip
#  rm hop-misc-static-schema-2.12.0-SNAPSHOT.zip
#  rm hop-transform-janino-2.12.0-SNAPSHOT.zip
#  rm hop-transform-javascript-2.12.0-SNAPSHOT.zip
#  rm hop-transform-script-2.12.0-SNAPSHOT.zip
#  rm hop-transform-databasejoin-2.12.0-SNAPSHOT.zip
#  rm hop-transform-sql-2.12.0-SNAPSHOT.zip
#  rm hop-action-sql-2.12.0-SNAPSHOT.zip
#  rm hop-action-waitforsql-2.12.0-SNAPSHOT.zip
else
    echo "Do nothing for now"
fi


$HOP_DEPLOY/hop-gui.sh debug
