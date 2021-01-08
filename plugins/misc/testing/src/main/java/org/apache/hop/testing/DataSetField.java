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

package org.apache.hop.testing;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class DataSetField {
  @HopMetadataProperty( key = "field_name" )
  private String fieldName;

  @HopMetadataProperty( key = "field_type" )
  private int type;

  @HopMetadataProperty( key = "field_length" )
  private int length;

  @HopMetadataProperty( key = "field_precision" )
  private int precision;

  @HopMetadataProperty( key = "field_comment" )
  private String comment;

  @HopMetadataProperty( key = "field_format" )
  private String format;

  public DataSetField() {
    // Empty constructor for MetaStoreFactory.
  }

  public DataSetField( String fieldName, int type, int length, int precision, String comment, String format ) {
    super();
    this.fieldName = fieldName;
    this.type = type;
    this.length = length;
    this.precision = precision;
    this.comment = comment;
    this.format = format;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    DataSetField that = (DataSetField) o;
    return Objects.equals( fieldName, that.fieldName );
  }

  @Override public int hashCode() {
    return Objects.hash( fieldName );
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public int getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( int type ) {
    this.type = type;
  }

  /**
   * Gets length
   *
   * @return value of length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length The length to set
   */
  public void setLength( int length ) {
    this.length = length;
  }

  /**
   * Gets precision
   *
   * @return value of precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision The precision to set
   */
  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  /**
   * Gets comment
   *
   * @return value of comment
   */
  public String getComment() {
    return comment;
  }

  /**
   * @param comment The comment to set
   */
  public void setComment( String comment ) {
    this.comment = comment;
  }

  /**
   * Gets format
   *
   * @return value of format
   */
  public String getFormat() {
    return format;
  }

  /**
   * @param format The format to set
   */
  public void setFormat( String format ) {
    this.format = format;
  }
}
