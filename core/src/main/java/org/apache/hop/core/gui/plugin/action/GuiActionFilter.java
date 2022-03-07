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

package org.apache.hop.core.gui.plugin.action;

public class GuiActionFilter {
  private String id;
  private String guiPluginClassName;
  private String guiPluginMethodName;
  private ClassLoader classLoader;

  public GuiActionFilter() {}

  public GuiActionFilter(
      String id, String guiPluginClassName, String guiPluginMethodName, ClassLoader classLoader) {
    this.id = id;
    this.guiPluginClassName = guiPluginClassName;
    this.guiPluginMethodName = guiPluginMethodName;
    this.classLoader = classLoader;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets guiPluginClassName
   *
   * @return value of guiPluginClassName
   */
  public String getGuiPluginClassName() {
    return guiPluginClassName;
  }

  /** @param guiPluginClassName The guiPluginClassName to set */
  public void setGuiPluginClassName(String guiPluginClassName) {
    this.guiPluginClassName = guiPluginClassName;
  }

  /**
   * Gets guiPluginMethodName
   *
   * @return value of guiPluginMethodName
   */
  public String getGuiPluginMethodName() {
    return guiPluginMethodName;
  }

  /** @param guiPluginMethodName The guiPluginMethodName to set */
  public void setGuiPluginMethodName(String guiPluginMethodName) {
    this.guiPluginMethodName = guiPluginMethodName;
  }

  /**
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /** @param classLoader The classLoader to set */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }
}
