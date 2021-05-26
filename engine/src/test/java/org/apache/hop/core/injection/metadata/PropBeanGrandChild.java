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
 */

package org.apache.hop.core.injection.metadata;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class PropBeanGrandChild {
  @HopMetadataProperty(key = "grand_child_name")
  private String grandChildName;

  @HopMetadataProperty(key = "grand_child_description")
  private String grandChildDescription;

  public PropBeanGrandChild() {}

  public PropBeanGrandChild(String grandChildName, String grandChildDescription) {
    this.grandChildName = grandChildName;
    this.grandChildDescription = grandChildDescription;
  }

  /**
   * Gets grandChildName
   *
   * @return value of grandChildName
   */
  public String getGrandChildName() {
    return grandChildName;
  }

  /** @param grandChildName The grandChildName to set */
  public void setGrandChildName(String grandChildName) {
    this.grandChildName = grandChildName;
  }

  /**
   * Gets grandChildDescription
   *
   * @return value of grandChildDescription
   */
  public String getGrandChildDescription() {
    return grandChildDescription;
  }

  /** @param grandChildDescription The grandChildDescription to set */
  public void setGrandChildDescription(String grandChildDescription) {
    this.grandChildDescription = grandChildDescription;
  }
}
