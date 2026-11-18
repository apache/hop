/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lineage.openlineage;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageSqlTables;

/**
 * Lets the rest of Hop find the tables in SQL as the OpenLineage sink does, for example the tables
 * a Table input queries when a data catalog is asked about a pipeline.
 */
@ExtensionPoint(
    id = "RegisterSqlTablesExtensionPoint",
    extensionPointId = "HopEnvironmentAfterInit",
    description = "Finds the tables in SQL with the OpenLineage SQL parser")
public class RegisterSqlTablesExtensionPoint implements IExtensionPoint<PluginRegistry> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, PluginRegistry registry)
      throws HopException {
    LineageSqlTables.register(parser());
  }

  static LineageSqlTables.IParser parser() {
    RelationalSqlParser sqlParser = RelationalSqlParser.create(true, null);
    return (sql, datasetNamespace) -> {
      RelationalSqlParser.Tables tables =
          sqlParser.parse(sql, OpenLineageEventMapper.sqlDialect(datasetNamespace));
      return new LineageSqlTables.Tables(tables.inputs(), tables.outputs());
    };
  }
}
