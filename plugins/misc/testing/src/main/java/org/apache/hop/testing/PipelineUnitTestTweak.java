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

public class PipelineUnitTestTweak {

  @HopMetadataProperty
  private PipelineTweak tweak;

  @HopMetadataProperty
  private String transformName;

  public PipelineUnitTestTweak() {
    tweak = PipelineTweak.NONE;
  }

  public PipelineUnitTestTweak( PipelineTweak tweak, String transformName ) {
    super();
    this.tweak = tweak;
    this.transformName = transformName;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( !( obj instanceof PipelineUnitTestTweak ) ) {
      return false;
    }
    if ( obj == this ) {
      return true;
    }

    PipelineUnitTestTweak other = (PipelineUnitTestTweak) obj;

    return transformName == null ? false : transformName.equals( other.transformName );
  }

  @Override
  public int hashCode() {
    return transformName == null ? 0 : transformName.hashCode();
  }

  public PipelineTweak getTweak() {
    return tweak;
  }

  public void setTweak( PipelineTweak tweak ) {
    this.tweak = tweak;
  }

  public String getTransformName() {
    return transformName;
  }

  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }
}
