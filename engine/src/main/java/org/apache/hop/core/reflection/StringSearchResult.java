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

package org.apache.hop.core.reflection;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;

public class StringSearchResult {
  private static final Class<?> PKG = Const.class; // For Translator

  private String string;
  private Object parentObject;
  private String fieldName;
  private Object grandParentObject;

  /**
   * @param string
   * @param parentObject
   */
  public StringSearchResult( String string, Object parentObject, Object grandParentObject, String fieldName ) {
    super();

    this.string = string;
    this.parentObject = parentObject;
    this.grandParentObject = grandParentObject;
    this.fieldName = fieldName;
  }

  public Object getParentObject() {
    return parentObject;
  }

  public void setParentObject( Object parentObject ) {
    this.parentObject = parentObject;
  }

  public String getString() {
    return string;
  }

  public void setString( String string ) {
    this.string = string;
  }

  public static final IRowMeta getResultRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString(
      BaseMessages.getString( PKG, "SearchResult.PipelineOrWorkflow" ) ) );
    rowMeta.addValueMeta( new ValueMetaString(
      BaseMessages.getString( PKG, "SearchResult.TransformDatabaseNotice" ) ) );
    rowMeta.addValueMeta( new ValueMetaString(
      BaseMessages.getString( PKG, "SearchResult.String" ) ) );
    rowMeta.addValueMeta( new ValueMetaString(
      BaseMessages.getString( PKG, "SearchResult.FieldName" ) ) );
    return rowMeta;
  }

  public Object[] toRow() {
    return new Object[] { grandParentObject.toString(), parentObject.toString(), string, fieldName, };
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append( parentObject.toString() ).append( " : " ).append( string );
    sb.append( " (" ).append( fieldName ).append( ")" );
    return sb.toString();
  }

  /**
   * @return Returns the fieldName.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return the grandParentObject
   */
  public Object getGrandParentObject() {
    return grandParentObject;
  }

  /**
   * @param grandParentObject the grandParentObject to set
   */
  public void setGrandParentObject( Object grandParentObject ) {
    this.grandParentObject = grandParentObject;
  }
}
