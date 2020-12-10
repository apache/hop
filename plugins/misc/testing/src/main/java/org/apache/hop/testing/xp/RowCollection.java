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

package org.apache.hop.testing.xp;

import org.apache.hop.core.row.IRowMeta;

import java.util.ArrayList;
import java.util.List;

public class RowCollection {

  private IRowMeta rowMeta;
  private List<Object[]> rows;

  public RowCollection() {
    rowMeta = null;
    rows = new ArrayList<>();
  }

  public RowCollection( IRowMeta rowMeta, List<Object[]> rows ) {
    this.rowMeta = rowMeta;
    this.rows = rows;
  }

  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  public void setRowMeta( IRowMeta rowMeta ) {
    this.rowMeta = rowMeta;
  }

  public List<Object[]> getRows() {
    return rows;
  }

  public void setRows( List<Object[]> rows ) {
    this.rows = rows;
  }
}
