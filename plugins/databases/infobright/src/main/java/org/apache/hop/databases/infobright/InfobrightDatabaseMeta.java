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

package org.apache.hop.databases.infobright;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.databases.mysql.MySqlDatabaseMeta;

@DatabaseMetaPlugin(
  type = "INFOBRIGHT",
  typeDescription = "Infobright"
)
@GuiPlugin( id = "GUI-InfobrightDatabaseMeta" )
public class InfobrightDatabaseMeta extends MySqlDatabaseMeta {

  @Override
  public int getDefaultDatabasePort() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE ) {
      return 5029;
    }
    return -1;
  }

  @Override
  public void addDefaultOptions() {
    addExtraOption( getPluginId(), "characterEncoding", "UTF-8" );
  }

}
