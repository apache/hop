/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.hop.core.injection.Injection;

public class Aggregation implements Cloneable {

  @Injection( name = "AGG_FIELD", group = "AGGREGATIONS" )
  private String field;

  @Injection( name = "AGG_SUBJECT", group = "AGGREGATIONS" )
  private String subject;

  private int type;

  @Injection( name = "AGG_VALUE", group = "AGGREGATIONS" )
  private String value;

  public Aggregation() {
  }

  public Aggregation( String field, String subject, int type, String value ) {
    this.field = field;
    this.subject = subject;
    this.type = type;
    this.value = value;
  }

  @Override public Aggregation clone() {
    return new Aggregation(field, subject, type, value);
  }

  @Injection( name = "AGG_TYPE", group = "AGGREGATIONS" )
  public void setType(String typeCode) {
    this.type = GroupByMeta.getType( typeCode );
  }

  /**
   * Gets field
   *
   * @return value of field
   */
  public String getField() {
    return field;
  }

  /**
   * @param field The field to set
   */
  public void setField( String field ) {
    this.field = field;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  public String getSubject() {
    return subject;
  }

  /**
   * @param subject The subject to set
   */
  public void setSubject( String subject ) {
    this.subject = subject;
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
   * Gets value
   *
   * @return value of value
   */
  public String getValue() {
    return value;
  }

  /**
   * @param value The value to set
   */
  public void setValue( String value ) {
    this.value = value;
  }
}
