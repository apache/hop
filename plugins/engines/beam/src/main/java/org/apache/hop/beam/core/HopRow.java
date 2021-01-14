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

package org.apache.hop.beam.core;

import java.io.Serializable;

public class HopRow implements Serializable {

  private Object[] row;

  public HopRow() {
  }

  public HopRow( Object[] row ) {
    this.row = row;
  }


  @Override public boolean equals( Object obj ) {
    if (!(obj instanceof HopRow )) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    HopRow otherHopRow = (HopRow) obj;

    Object[] thisRow = row;
    Object[] otherRow = otherHopRow.getRow();
    if (thisRow==null && otherRow==null) {
      return true;
    }
    if ( (thisRow==null && otherRow!=null) || (thisRow!=null && otherRow==null)) {
      return false;
    }
    if (thisRow.length!=otherRow.length) {
      return false;
    }
    for (int i=0;i<thisRow.length;i++) {
      Object thisValue = thisRow[i];
      Object otherValue = otherRow[i];
      if ((thisValue==null && otherValue!=null ) || (thisValue!=null && otherValue==null)) {
        return false;
      }
      if (thisValue!=null && otherValue!=null && !thisValue.equals( otherValue )) {
        return false;
      }
    }
    return true;
  }

  @Override public int hashCode() {
    if (row==null) {
      return 0;
    }
    int hashValue = 0;
    for (int i=0;i<row.length;i++) {
      if (row[i]!=null) {
        hashValue^=row[i].hashCode();
      }
    }
    return hashValue;
  }

  public boolean allNull() {
    if (row==null) {
      return true;
    }
    for (int i=0;i<row.length;i++) {
      if (row[i]!=null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets row
   *
   * @return value of row
   */
  public Object[] getRow() {
    return row;
  }

  /**
   * @param row The row to set
   */
  public void setRow( Object[] row ) {
    this.row = row;
  }
}
