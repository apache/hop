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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

public class KeyField {
  @HopMetadataProperty(
      injectionKey = "KEY_FIELD",
      injectionKeyDescription = "CombinationLookup.Injection.KEY_FIELD",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  String name;

  @HopMetadataProperty(
      injectionKey = "KEY_LOOKUP",
      injectionKeyDescription = "CombinationLookup.Injection.KEY_LOOKUP")
  String lookup;

  public KeyField() {}

  public KeyField(String name, String lookup) {
    this.name = name;
    this.lookup = lookup;
  }

  public KeyField(KeyField f) {
    this.name = f.name;
    this.lookup = f.lookup;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets lookup
   *
   * @return value of lookup
   */
  public String getLookup() {
    return lookup;
  }

  /**
   * Sets lookup
   *
   * @param lookup value of lookup
   */
  public void setLookup(String lookup) {
    this.lookup = lookup;
  }
}
