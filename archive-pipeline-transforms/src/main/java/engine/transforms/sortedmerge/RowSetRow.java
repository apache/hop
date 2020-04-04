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

package org.apache.hop.pipeline.transforms.sortedmerge;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.row.IRowMeta;

public class RowSetRow {
  private RowSet rowSet;
  private IRowMeta rowMeta;
  private Object[] rowData;

  /**
   * @param rowSet
   * @param rowData
   */
  public RowSetRow( RowSet rowSet, IRowMeta rowMeta, Object[] rowData ) {
    super();
    this.rowSet = rowSet;
    this.rowMeta = rowMeta;
    this.rowData = rowData;
  }

  /**
   * @return the rowSet
   */
  public RowSet getRowSet() {
    return rowSet;
  }

  /**
   * @param rowSet the rowSet to set
   */
  public void setRowSet( RowSet rowSet ) {
    this.rowSet = rowSet;
  }

  /**
   * @return the rowData
   */
  public Object[] getRowData() {
    return rowData;
  }

  /**
   * @param rowData the rowData to set
   */
  public void setRowData( Object[] rowData ) {
    this.rowData = rowData;
  }

  /**
   * @return the rowMeta
   */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /**
   * @param rowMeta the rowMeta to set
   */
  public void setRowMeta( IRowMeta rowMeta ) {
    this.rowMeta = rowMeta;
  }
}
