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


import org.apache.hop.core.injection.Injection;

public class MetaInjectMapping {

  @Injection( name = "MAPPING_SOURCE_TRANSFORM", group = "MAPPING_FIELDS" )
  private String sourceTransform;

  @Injection( name = "MAPPING_SOURCE_FIELD", group = "MAPPING_FIELDS" )
  private String sourceField;

  @Injection( name = "MAPPING_TARGET_TRANSFORM", group = "MAPPING_FIELDS" )
  private String targetTransform;

  @Injection( name = "MAPPING_TARGET_FIELD", group = "MAPPING_FIELDS" )
  private String targetField;

  public MetaInjectMapping() {
  }

  public String getSourceTransform() {
    return sourceTransform;
  }

  public void setSourceTransform(String sourceTransform ) {
    this.sourceTransform = sourceTransform;
  }

  public String getSourceField() {
    return sourceField;
  }

  public void setSourceField( String sourceField ) {
    this.sourceField = sourceField;
  }

  public String getTargetTransform() {
    return targetTransform;
  }

  public void setTargetTransform(String targetTransform) {
    this.targetTransform = targetTransform;
  }

  public String getTargetField() {
    return targetField;
  }

  public void setTargetField( String targetField ) {
    this.targetField = targetField;
  }

}
