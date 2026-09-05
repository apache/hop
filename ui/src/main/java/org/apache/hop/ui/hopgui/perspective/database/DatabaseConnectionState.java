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
import lombok.Setter;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaInformation;

/**
 * In-memory state for one project connection in the database tree. "Connected" means schema/table
 * metadata has been loaded; JDBC is not kept open (each operation uses its own {@code Database}).
 */
@Getter
@Setter
public class DatabaseConnectionState {
  private DatabaseMeta databaseMeta;
  private boolean connected;
  private DatabaseMetaInformation information;

  public DatabaseConnectionState(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }
}
