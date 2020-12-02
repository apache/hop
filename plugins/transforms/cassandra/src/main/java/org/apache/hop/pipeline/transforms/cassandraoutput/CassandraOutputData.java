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
package org.apache.hop.pipeline.transforms.cassandraoutput;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Data class for the CassandraOutput step. Contains methods for obtaining a connection to
 * cassandra, creating a new table, updating a table's meta data and constructing a batch insert CQL
 * statement.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraOutputData extends BaseTransformData implements ITransformData {

  /** The output data format */
  protected IRowMeta m_outputRowMeta;

  /**
   * Get the output row format
   *
   * @return the output row format
   */
  public IRowMeta getOutputRowMeta() {
    return m_outputRowMeta;
  }

  /**
   * Set the output row format
   *
   * @param rmi the output row format
   */
  public void setOutputRowMeta(IRowMeta rmi) {
    m_outputRowMeta = rmi;
  }
}
