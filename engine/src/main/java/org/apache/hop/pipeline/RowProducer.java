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

package org.apache.hop.pipeline;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransform;

import java.util.concurrent.TimeUnit;

/**
 * Allows you to "Inject" rows into a transform.
 *
 * @author Matt
 */
public class RowProducer {
  private IRowSet rowSet;
  private ITransform iTransform;

  public RowProducer( ITransform iTransform, IRowSet rowSet ) {
    this.iTransform = iTransform;
    this.rowSet = rowSet;
  }

  /**
   * Puts a row into the underlying row set. This will block until the row is successfully added.
   *
   * @see #putRow(IRowMeta, Object[], boolean) putRow(IRowMeta, Object[], true)
   */
  public void putRow( IRowMeta rowMeta, Object[] row ) {
    putRow( rowMeta, row, true );
  }

  /**
   * Puts a row on to the underlying row set, optionally blocking until the row can be successfully put.
   *
   * @return true if the row was successfully added to the rowset and false if this buffer was full. If {@code block} is
   * true this will always return true.
   * @see IRowSet#putRow(IRowMeta, Object[])
   */
  public boolean putRow( IRowMeta rowMeta, Object[] row, boolean block ) {
    if ( block ) {
      boolean added = false;
      while ( !added ) {
        added = rowSet.putRowWait( rowMeta, row, Long.MAX_VALUE, TimeUnit.DAYS );
      }

      return true;
    }
    return rowSet.putRow( rowMeta, row );
  }

  /**
   * @see IRowSet#putRowWait(IRowMeta, Object[], long, TimeUnit)
   */
  public boolean putRowWait( IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu ) {
    return rowSet.putRowWait( rowMeta, rowData, time, tu );
  }

  /**
   * Signal that we are done producing rows.
   * It will allow the transform to which this producer is attached to know that no more rows are forthcoming.
   */
  public void finished() {
    rowSet.setDone();
  }

  /**
   * @return Returns the rowSet.
   */
  public IRowSet getRowSet() {
    return rowSet;
  }

  /**
   * @param rowSet The rowSet to set.
   */
  public void setRowSet( IRowSet rowSet ) {
    this.rowSet = rowSet;
  }

  /**
   * @return Returns the iTransform.
   */
  public ITransform getTransformInterface() {
    return iTransform;
  }

  /**
   * @param iTransform The iTransform to set.
   */
  public void setTransformInterface( ITransform iTransform ) {
    this.iTransform = iTransform;
  }

}
