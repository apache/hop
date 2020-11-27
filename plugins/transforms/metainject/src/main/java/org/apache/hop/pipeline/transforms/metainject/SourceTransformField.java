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

package org.apache.hop.pipeline.transforms.metainject;

public class SourceTransformField {

  private String transformName;
  private String field;

  /**
   * @param transformName
   * @param field
   */
  public SourceTransformField(String transformName, String field ) {
    this.transformName = transformName;
    this.field = field;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( !( obj instanceof SourceTransformField) ) {
      return false;
    }
    if ( obj == this ) {
      return true;
    }

    SourceTransformField source = (SourceTransformField) obj;
    return transformName.equalsIgnoreCase( source.getTransformName() ) && field.equals( source.getField() );
  }

  @Override
  public int hashCode() {
    return transformName.hashCode() ^ field.hashCode();
  }

  /**
   * @return the transform name
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * @param transformName
   *          the transform name to set
   */
  public void setTransformName(String transformName ) {
    this.transformName = transformName;
  }

  /**
   * @return the field
   */
  public String getField() {
    return field;
  }

  /**
   * @param field
   *          the field to set
   */
  public void setField( String field ) {
    this.field = field;
  }

}
