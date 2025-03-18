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

package org.apache.hop.core.gui.plugin.toolbar;

import java.lang.reflect.Method;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.BaseGuiElements;

/** This represents a list of GUI elements under a certain heading or ID */
@Getter
@Setter
public class GuiToolbarItem extends BaseGuiElements implements Comparable<GuiToolbarItem> {

  private String root;
  private String id;
  private String label;
  private String toolTip;
  private GuiToolbarElementType type;
  private String image;
  private String imageMethod;
  private boolean password;
  private String getComboValuesMethod;
  private boolean ignored;
  private boolean addingSeparator;
  private ClassLoader classLoader;

  // The singleton listener class to use
  private boolean singleTon;
  private String listenerClass;
  private String listenerMethod;

  private int extraWidth;
  private boolean alignRight;
  private boolean readOnly;

  public GuiToolbarItem() {}

  public GuiToolbarItem(
      GuiToolbarElement toolbarElement,
      String listenerClass,
      Method method,
      ClassLoader classLoader) {
    this();

    this.root = toolbarElement.root();
    this.id = toolbarElement.id();
    this.type = toolbarElement.type();
    this.getComboValuesMethod = toolbarElement.comboValuesMethod();
    this.image = toolbarElement.image();
    this.imageMethod = toolbarElement.imageMethod();
    this.password = toolbarElement.password();
    this.ignored = toolbarElement.ignored();
    this.addingSeparator = toolbarElement.separator();
    this.singleTon = true;
    this.listenerClass = listenerClass;
    this.listenerMethod = method.getName();
    this.label =
        getTranslation(
            toolbarElement.label(),
            method.getDeclaringClass().getPackage().getName(),
            method.getDeclaringClass());
    this.toolTip =
        getTranslation(
            toolbarElement.toolTip(),
            method.getDeclaringClass().getPackage().getName(),
            method.getDeclaringClass());
    this.classLoader = classLoader;
    this.extraWidth = toolbarElement.extraWidth();
    this.alignRight = toolbarElement.alignRight();
    this.readOnly = toolbarElement.readOnly();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GuiToolbarItem that = (GuiToolbarItem) o;
    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "GuiToolbarItem{" + "id='" + id + '\'' + '}';
  }

  @Override
  public int compareTo(GuiToolbarItem o) {
    return id.compareTo(o.id);
  }
}
