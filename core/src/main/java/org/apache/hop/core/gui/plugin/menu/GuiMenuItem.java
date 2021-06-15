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

package org.apache.hop.core.gui.plugin.menu;

import org.apache.hop.core.gui.plugin.BaseGuiElements;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * This represents a list of GUI Menu elements under a certain heading or ID
 */
public class GuiMenuItem extends BaseGuiElements implements Comparable<GuiMenuItem> {

  private String root;

  private String id;

  private String parentId;

  private String label;

  private String toolTip;

  private GuiMenuElementType type;

  private String image;
  private String disabledImage;

  private boolean ignored;

  private boolean addingSeparator;

  private ClassLoader classLoader;

  private boolean singleTon;
  private String listenerClassName;
  private String listenerMethod;

  public GuiMenuItem() {
  }


  public GuiMenuItem( GuiMenuElement guiElement, Method guiPluginMethod, String guiPluginClassName, ClassLoader classLoader ) {
    this();

    this.root = guiElement.root();
    this.id = guiElement.id();
    this.type = guiElement.type();
    this.parentId = guiElement.parentId();
    this.image = guiElement.image();
    this.ignored = guiElement.ignored();
    this.addingSeparator = guiElement.separator();
    this.listenerMethod = guiPluginMethod.getName();
    this.listenerClassName = guiPluginClassName; // Ask the classloader for the class
    this.singleTon = true; // Always a singleton for now
    this.label = getTranslation(guiElement.label(), guiPluginMethod.getDeclaringClass().getPackage().getName(), guiPluginMethod.getDeclaringClass() );
    this.toolTip = getTranslation(guiElement.toolTip(), guiPluginMethod.getDeclaringClass().getPackage().getName(), guiPluginMethod.getDeclaringClass() );
    this.classLoader = classLoader;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    GuiMenuItem that = (GuiMenuItem) o;
    return id.equals( that.id );
  }

  @Override public int hashCode() {
    return Objects.hash( id );
  }

  @Override public int compareTo( GuiMenuItem o ) {
    return id.compareTo( o.id );
  }

  @Override public String toString() {
    return "GuiMenuItem{" +
      "id='" + id + '\'' +
      '}';
  }

  /**
   * Gets root
   *
   * @return value of root
   */
  public String getRoot() {
    return root;
  }

  /**
   * @param root The root to set
   */
  public void setRoot( String root ) {
    this.root = root;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets parentId
   *
   * @return value of parentId
   */
  public String getParentId() {
    return parentId;
  }

  /**
   * @param parentId The parentId to set
   */
  public void setParentId( String parentId ) {
    this.parentId = parentId;
  }

  /**
   * Gets label
   *
   * @return value of label
   */
  public String getLabel() {
    return label;
  }

  /**
   * @param label The label to set
   */
  public void setLabel( String label ) {
    this.label = label;
  }

  /**
   * Gets toolTip
   *
   * @return value of toolTip
   */
  public String getToolTip() {
    return toolTip;
  }

  /**
   * @param toolTip The toolTip to set
   */
  public void setToolTip( String toolTip ) {
    this.toolTip = toolTip;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public GuiMenuElementType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( GuiMenuElementType type ) {
    this.type = type;
  }

  /**
   * Gets image
   *
   * @return value of image
   */
  public String getImage() {
    return image;
  }

  /**
   * @param image The image to set
   */
  public void setImage( String image ) {
    this.image = image;
  }

  /**
   * Gets disabledImage
   *
   * @return value of disabledImage
   */
  public String getDisabledImage() {
    return disabledImage;
  }

  /**
   * @param disabledImage The disabledImage to set
   */
  public void setDisabledImage( String disabledImage ) {
    this.disabledImage = disabledImage;
  }

  /**
   * Gets ignored
   *
   * @return value of ignored
   */
  public boolean isIgnored() {
    return ignored;
  }

  /**
   * @param ignored The ignored to set
   */
  public void setIgnored( boolean ignored ) {
    this.ignored = ignored;
  }

  /**
   * Gets addingSeparator
   *
   * @return value of addingSeparator
   */
  public boolean isAddingSeparator() {
    return addingSeparator;
  }

  /**
   * @param addingSeparator The addingSeparator to set
   */
  public void setAddingSeparator( boolean addingSeparator ) {
    this.addingSeparator = addingSeparator;
  }

  /**
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * @param classLoader The classLoader to set
   */
  public void setClassLoader( ClassLoader classLoader ) {
    this.classLoader = classLoader;
  }

  /**
   * Gets singleTon
   *
   * @return value of singleTon
   */
  public boolean isSingleTon() {
    return singleTon;
  }

  /**
   * @param singleTon The singleTon to set
   */
  public void setSingleTon( boolean singleTon ) {
    this.singleTon = singleTon;
  }

  /**
   * Gets listenerClassName
   *
   * @return value of listenerClassName
   */
  public String getListenerClassName() {
    return listenerClassName;
  }

  /**
   * @param listenerClassName The listenerClassName to set
   */
  public void setListenerClassName( String listenerClassName ) {
    this.listenerClassName = listenerClassName;
  }

  /**
   * Gets listenerMethod
   *
   * @return value of listenerMethod
   */
  public String getListenerMethod() {
    return listenerMethod;
  }

  /**
   * @param listenerMethod The listenerMethod to set
   */
  public void setListenerMethod( String listenerMethod ) {
    this.listenerMethod = listenerMethod;
  }
}
