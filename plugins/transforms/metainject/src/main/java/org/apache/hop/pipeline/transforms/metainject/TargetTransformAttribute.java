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

public class TargetTransformAttribute {

  private String transformName;
  private String attributeKey;
  private boolean detail;

  /**
   * @param transformName
   * @param attributeKey
   */
  public TargetTransformAttribute(String transformName, String attributeKey, boolean detail ) {
    this.transformName = transformName;
    this.attributeKey = attributeKey;
    this.detail = detail;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( !( obj instanceof TargetTransformAttribute) ) {
      return false;
    }
    if ( obj == this ) {
      return true;
    }

    TargetTransformAttribute target = (TargetTransformAttribute) obj;
    return transformName.equalsIgnoreCase( target.getTransformName() ) && attributeKey.equals( target.getAttributeKey() );
  }

  @Override
  public int hashCode() {
    return transformName.hashCode() ^ attributeKey.hashCode();
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
   * @return the attributeKey
   */
  public String getAttributeKey() {
    return attributeKey;
  }

  /**
   * @param attributeKey
   *          the attributeKey to set
   */
  public void setAttributeKey( String attributeKey ) {
    this.attributeKey = attributeKey;
  }

  public void setDetail( boolean detail ) {
    this.detail = detail;
  }

  public boolean isDetail() {
    return detail;
  }

}
