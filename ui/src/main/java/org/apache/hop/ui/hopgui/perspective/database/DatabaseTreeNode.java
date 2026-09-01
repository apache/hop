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

package org.apache.hop.ui.hopgui.perspective.database;

import lombok.Getter;

/** Data stored on a connection-tree item. */
@Getter
public class DatabaseTreeNode {

  public enum Kind {
    CONNECTION,
    SCHEMA,
    CATALOG,
    FOLDER,
    TABLE,
    VIEW,
    SYNONYM
  }

  private final Kind kind;
  private final String connectionName;
  private final String schemaName;
  private final String objectName;
  private final boolean connected;

  public DatabaseTreeNode(
      Kind kind, String connectionName, String schemaName, String objectName, boolean connected) {
    this.kind = kind;
    this.connectionName = connectionName;
    this.schemaName = schemaName;
    this.objectName = objectName;
    this.connected = connected;
  }

  public static DatabaseTreeNode connection(String name, boolean connected) {
    return new DatabaseTreeNode(Kind.CONNECTION, name, null, name, connected);
  }

  public static DatabaseTreeNode schema(String connectionName, String schemaName) {
    return new DatabaseTreeNode(Kind.SCHEMA, connectionName, schemaName, schemaName, true);
  }

  public static DatabaseTreeNode catalog(String connectionName, String catalogName) {
    return new DatabaseTreeNode(Kind.CATALOG, connectionName, catalogName, catalogName, true);
  }

  public static DatabaseTreeNode folder(String connectionName, String folderName) {
    return new DatabaseTreeNode(Kind.FOLDER, connectionName, null, folderName, true);
  }

  public static DatabaseTreeNode table(
      Kind kind, String connectionName, String schemaName, String tableName) {
    return new DatabaseTreeNode(kind, connectionName, schemaName, tableName, true);
  }

  public boolean isTableLike() {
    return kind == Kind.TABLE || kind == Kind.VIEW || kind == Kind.SYNONYM;
  }
}
