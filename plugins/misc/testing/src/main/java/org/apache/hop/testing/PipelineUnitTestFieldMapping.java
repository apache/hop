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

/**
 * This class simply describes a mapping between the transform fields we want to test and the data set fields you want to match with.
 *
 * @author matt
 */
public class PipelineUnitTestFieldMapping {

  @HopMetadataProperty( key = "transform_field" )
  private String transformFieldName;

  @HopMetadataProperty( key = "data_set_field" )
  private String dataSetFieldName;

  public PipelineUnitTestFieldMapping() {
  }

  public PipelineUnitTestFieldMapping( String transformFieldName, String dataSetFieldName ) {
    this();
    this.transformFieldName = transformFieldName;
    this.dataSetFieldName = dataSetFieldName;
  }

  public String getTransformFieldName() {
    return transformFieldName;
  }

  public void setTransformFieldName( String transformFieldName ) {
    this.transformFieldName = transformFieldName;
  }

  public String getDataSetFieldName() {
    return dataSetFieldName;
  }

  public void setDataSetFieldName( String dataSetFieldName ) {
    this.dataSetFieldName = dataSetFieldName;
  }
}
