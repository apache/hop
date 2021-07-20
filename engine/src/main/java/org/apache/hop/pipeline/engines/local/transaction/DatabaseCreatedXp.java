/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.engines.local.transaction;

import org.apache.hop.core.Const;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

@ExtensionPoint(
    id = "DatabaseCreatedXp",
    extensionPointId = "DatabaseCreated",
    description =
        "Pass the connection group to a relational database from a parent pipeline, workflow, action or transform")
public class DatabaseCreatedXp implements IExtensionPoint<Database> {
  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Database database)
      throws HopException {

    // Can we figure out the connection group?
    //
    if (!(database.getParentObject() instanceof IExtensionData)) {
      return;
    }

    IExtensionData extensionData = (IExtensionData) database.getParentObject();
    String connectionGroup =
        (String) extensionData.getExtensionDataMap().get(Const.CONNECTION_GROUP);
    if (connectionGroup != null) {
      database.setConnectionGroup(connectionGroup);
    }
  }
}
