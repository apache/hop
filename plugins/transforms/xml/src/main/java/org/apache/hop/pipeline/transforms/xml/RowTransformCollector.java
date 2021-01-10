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

package org.apache.hop.pipeline.transforms.xml;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.IRowListener;

import java.util.ArrayList;
import java.util.List;


/**
 * Helper class for testcases. You can add an instance of this class to a transform to read all of the Rows the transform read or
 * wrote.
 * 
 * @author Sven Boden
 */
public class RowTransformCollector implements IRowListener {
  private List<RowMetaAndData> rowsRead;
  private List<RowMetaAndData> rowsWritten;
  private List<RowMetaAndData> rowsError;

  public RowTransformCollector() {
    rowsRead = new ArrayList<>();
    rowsWritten = new ArrayList<>();
    rowsError = new ArrayList<>();
  }

  public void rowReadEvent( IRowMeta rowMeta, Object[] row ) {
    rowsRead.add( new RowMetaAndData( rowMeta, row ) );
  }

  public void rowWrittenEvent(IRowMeta rowMeta, Object[] row ) {
    rowsWritten.add( new RowMetaAndData( rowMeta, row ) );
  }

  public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) {
    rowsError.add( new RowMetaAndData( rowMeta, row ) );
  }

  /**
   * Clear the rows read and rows written.
   */
  public void clear() {
    rowsRead.clear();
    rowsWritten.clear();
    rowsError.clear();
  }

  public List<RowMetaAndData> getRowsRead() {
    return rowsRead;
  }

  public List<RowMetaAndData> getRowsWritten() {
    return rowsWritten;
  }

  public List<RowMetaAndData> getRowsError() {
    return rowsError;
  }
}
