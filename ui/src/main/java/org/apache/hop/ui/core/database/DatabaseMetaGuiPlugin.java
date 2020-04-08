/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.core.database;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Database Connection",
  description = "A relational database connection",
  iconImage = "ui/images/CNC.svg"
)
public class DatabaseMetaGuiPlugin implements IGuiMetaStorePlugin<DatabaseMeta> {

  @Override public Class<DatabaseMeta> getMetastoreElementClass() {
    return DatabaseMeta.class;
  }
}
