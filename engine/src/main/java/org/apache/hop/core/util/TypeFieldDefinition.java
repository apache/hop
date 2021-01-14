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

package org.apache.hop.core.util;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

public class TypeFieldDefinition {
  private int type;
  private String fieldName;

  /**
   * @param type
   * @param fieldName
   */
  public TypeFieldDefinition( int type, String fieldName ) {
    this.type = type;
    this.fieldName = fieldName;
  }

  /**
   * @return the type
   */
  public int getType() {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType( int type ) {
    this.type = type;
  }

  /**
   * @return the fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName the fieldName to set
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  public String getTypeDescription() {
    switch ( type ) {
      case IValueMeta.TYPE_BOOLEAN:
        return "boolean";
      case IValueMeta.TYPE_INTEGER:
        return "int";
      default:
        return ValueMetaFactory.getValueMetaName( type );
    }
  }

  public String getMemberName() {
    return fieldName.substring( 0, 1 ).toLowerCase() + fieldName.substring( 1 );
  }
}
