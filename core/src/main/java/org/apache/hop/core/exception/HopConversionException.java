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

package org.apache.hop.core.exception;

import org.apache.hop.core.row.IValueMeta;

import java.util.List;

public class HopConversionException extends HopException {

  private List<Exception> causes;
  private List<IValueMeta> fields;
  private Object[] rowData;

  /**
   *
   */
  private static final long serialVersionUID = 1697154653111622296L;

  /**
   * Constructs a new throwable with null as its detail message.
   */
  public HopConversionException() {
    super();
  }

  /**
   * Constructs a new throwable with the specified detail message and cause.
   *
   * @param message the detail message (which is saved for later retrieval by the getMessage() method).
   * @param causes  the causes of the conversion errors
   * @param fields  the failing fields
   * @param rowData the row with the failed fields set to null.
   */
  public HopConversionException( String message, List<Exception> causes, List<IValueMeta> fields,
                                 Object[] rowData ) {
    super( message );
    this.causes = causes;
    this.fields = fields;
    this.rowData = rowData;
  }

  /**
   * @return the causes
   */
  public List<Exception> getCauses() {
    return causes;
  }

  /**
   * @param causes the causes to set
   */
  public void setCauses( List<Exception> causes ) {
    this.causes = causes;
  }

  /**
   * @return the fields
   */
  public List<IValueMeta> getFields() {
    return fields;
  }

  /**
   * @param fields the fields to set
   */
  public void setFields( List<IValueMeta> fields ) {
    this.fields = fields;
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
}
