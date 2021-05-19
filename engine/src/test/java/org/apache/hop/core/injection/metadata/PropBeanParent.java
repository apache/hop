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

import java.util.ArrayList;
import java.util.List;

public class PropBeanParent {

  @HopMetadataProperty(key = "str")
  private String stringField;

  @HopMetadataProperty(key = "int")
  private int intField;

  @HopMetadataProperty(key = "long")
  private long longField;

  @HopMetadataProperty(key = "boolean")
  private boolean booleanField;

  @HopMetadataProperty(groupKey = "childGroup", key = "child")
  private PropBeanChild child;

  @HopMetadataProperty(groupKey = "entries", key = "entry")
  private List<PropBeanListChild> children;

  @HopMetadataProperty(groupKey = "strings", key = "string")
  private List<String> strings;

  public PropBeanParent() {
    children = new ArrayList<>();
    strings = new ArrayList<>();
  }

  /**
   * Gets stringField
   *
   * @return value of stringField
   */
  public String getStringField() {
    return stringField;
  }

  /** @param stringField The stringField to set */
  public void setStringField(String stringField) {
    this.stringField = stringField;
  }

  /**
   * Gets intField
   *
   * @return value of intField
   */
  public int getIntField() {
    return intField;
  }

  /** @param intField The intField to set */
  public void setIntField(int intField) {
    this.intField = intField;
  }

  /**
   * Gets longField
   *
   * @return value of longField
   */
  public long getLongField() {
    return longField;
  }

  /** @param longField The longField to set */
  public void setLongField(long longField) {
    this.longField = longField;
  }

  /**
   * Gets booleanField
   *
   * @return value of booleanField
   */
  public boolean isBooleanField() {
    return booleanField;
  }

  /** @param booleanField The booleanField to set */
  public void setBooleanField(boolean booleanField) {
    this.booleanField = booleanField;
  }

  /**
   * Gets child
   *
   * @return value of child
   */
  public PropBeanChild getChild() {
    return child;
  }

  /** @param child The child to set */
  public void setChild(PropBeanChild child) {
    this.child = child;
  }

  /**
   * Gets children
   *
   * @return value of children
   */
  public List<PropBeanListChild> getChildren() {
    return children;
  }

  /** @param children The children to set */
  public void setChildren(List<PropBeanListChild> children) {
    this.children = children;
  }

  /**
   * Gets strings
   *
   * @return value of strings
   */
  public List<String> getStrings() {
    return strings;
  }

  /** @param strings The strings to set */
  public void setStrings(List<String> strings) {
    this.strings = strings;
  }
}
