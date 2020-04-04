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

package org.apache.hop.core.logging;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.api.IMetaStore;

public interface ILogTablePlugin extends ILogTable {

  enum TableType {
    JOB, PIPELINE;
  }

  TableType getType();

  String getLogTablePluginUIClassname();

  void setContext( IVariables variables, IMetaStore metaStore );

  // Otherwise identical to the log table interface.

}
