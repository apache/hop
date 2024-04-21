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
 *
 */

package org.apache.hop.pipeline.transforms.valuemapper;

import org.apache.hop.metadata.api.HopMetadataProperty;

/** The values for mapping */
public class Values {

  @HopMetadataProperty(
      key = "source_value",
      injectionKey = "SOURCE",
      injectionKeyDescription = "ValueMapper.Injection.SOURCE")
  private String source;

  @HopMetadataProperty(
      key = "target_value",
      injectionKey = "TARGET",
      injectionKeyDescription = "ValueMapper.Injection.TARGET")
  private String target;

  public Values() {}

  public Values(Values other) {
    this.source = other.source;
    this.target = other.target;
  }

  public Values(String source, String target) {
    this.source = source;
    this.target = target;
  }

  /**
   * Gets source value
   *
   * @return value of source
   */
  public String getSource() {
    return source;
  }

  /**
   * Sets source value
   *
   * @param value value of source
   */
  public void setSource(String value) {
    this.source = value;
  }

  /**
   * Gets target value
   *
   * @return value of source
   */
  public String getTarget() {
    return target;
  }

  /**
   * Sets target value
   *
   * @param value value of target
   */
  public void setTarget(String value) {
    this.target = value;
  }
}
