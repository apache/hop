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

package org.apache.hop.core.gui.plugin.tab;

import java.lang.reflect.Method;
import org.apache.hop.core.gui.plugin.BaseGuiElements;

public class GuiTabItem extends BaseGuiElements implements Comparable<GuiTabItem> {

  private String id;
  private String parent;
  private String pluginClassName;
  private GuiTab guiTab;
  private String targetClass;
  private Method method;
  private ClassLoader classLoader;

  public GuiTabItem(
      String pluginClassName, GuiTab guiTab, Method tabMethod, ClassLoader classLoader) {
    this.id = guiTab.id();
    this.parent = guiTab.parentId();
    this.pluginClassName = pluginClassName;
    this.targetClass = guiTab.targetClass();
    this.guiTab = guiTab;
    this.method = tabMethod;
    this.classLoader = classLoader;
  }

  public String getPluginClassName() {
    return pluginClassName;
  }

  public void setPluginClassName(String pluginClassName) {
    this.pluginClassName = pluginClassName;
  }

  public GuiTab getGuiTab() {
    return guiTab;
  }

  public void setGuiTab(GuiTab guiTab) {
    this.guiTab = guiTab;
  }

  public Method getMethod() {
    return method;
  }

  public void setMethod(Method method) {
    this.method = method;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public String getTargetClass() {
    return targetClass;
  }

  public void setTargetClass(String targetClass) {
    this.targetClass = targetClass;
  }

  @Override
  public int compareTo(GuiTabItem guiTabItem) {
    return id.compareTo(guiTabItem.id);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getParent() {
    return parent;
  }

  public void setParent(String parent) {
    this.parent = parent;
  }
}
