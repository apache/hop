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

public class PropBeanChild {

  @HopMetadataProperty(key = "child1")
  private String childField1;

  @HopMetadataProperty(key = "child2")
  private String childField2;

  @HopMetadataProperty private PropBeanGrandChild grandChild;

  public PropBeanChild() {
    grandChild = new PropBeanGrandChild();
  }

  /**
   * Gets childField1
   *
   * @return value of childField1
   */
  public String getChildField1() {
    return childField1;
  }

  /** @param childField1 The childField1 to set */
  public void setChildField1(String childField1) {
    this.childField1 = childField1;
  }

  /**
   * Gets childField2
   *
   * @return value of childField2
   */
  public String getChildField2() {
    return childField2;
  }

  /** @param childField2 The childField2 to set */
  public void setChildField2(String childField2) {
    this.childField2 = childField2;
  }

  /**
   * Gets grandChild
   *
   * @return value of grandChild
   */
  public PropBeanGrandChild getGrandChild() {
    return grandChild;
  }

  /** @param grandChild The grandChild to set */
  public void setGrandChild(PropBeanGrandChild grandChild) {
    this.grandChild = grandChild;
  }
}
