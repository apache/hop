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

import org.apache.hop.core.gui.plugin.BaseGuiElements;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * This represents a list of GUI elements under a certain heading or ID
 */
public class GuiToolbarItem extends BaseGuiElements implements Comparable<GuiToolbarItem> {

  private String root;

  private String id;

  private String label;

  private String toolTip;

  private GuiToolbarElementType type;

  private String image;
  private String disabledImage;

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

  public GuiToolbarItem() {
  }

  public GuiToolbarItem( GuiToolbarElement toolbarElement, String listenerClass, Method method, ClassLoader classLoader ) {
    this();

    this.root = toolbarElement.root();
    this.id = toolbarElement.id();
    this.type = toolbarElement.type();
    this.getComboValuesMethod = toolbarElement.comboValuesMethod();
    this.image = toolbarElement.image();
    this.disabledImage = toolbarElement.disabledImage();
    this.password = toolbarElement.password();    
    this.ignored = toolbarElement.ignored();
    this.addingSeparator = toolbarElement.separator();
    this.singleTon = true;
    this.listenerClass = listenerClass;
    this.listenerMethod = method.getName();
    this.label = getTranslation( toolbarElement.label(), method.getDeclaringClass().getPackage().getName(), method.getDeclaringClass() );
    this.toolTip = getTranslation( toolbarElement.toolTip(), method.getDeclaringClass().getPackage().getName(), method.getDeclaringClass() );
    this.classLoader = classLoader;
    this.extraWidth = toolbarElement.extraWidth();
    this.alignRight = toolbarElement.alignRight();
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    GuiToolbarItem that = (GuiToolbarItem) o;
    return id.equals( that.id );
  }

  @Override public int hashCode() {
    return Objects.hash( id );
  }

  @Override public String toString() {
    return "GuiToolbarItem{" +
      "id='" + id + '\'' +
      '}';
  }

  @Override public int compareTo( GuiToolbarItem o ) {
    return id.compareTo( o.id );
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
  public GuiToolbarElementType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( GuiToolbarElementType type ) {
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
   * Gets password
   *
   * @return value of password
   */
  public boolean isPassword() {
    return password;
  }

  /**
   * @param password The password to set
   */
  public void setPassword( boolean password ) {
    this.password = password;
  }

  /**
   * Gets getComboValuesMethod
   *
   * @return value of getComboValuesMethod
   */
  public String getGetComboValuesMethod() {
    return getComboValuesMethod;
  }

  /**
   * @param getComboValuesMethod The getComboValuesMethod to set
   */
  public void setGetComboValuesMethod( String getComboValuesMethod ) {
    this.getComboValuesMethod = getComboValuesMethod;
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
   * Gets listenerClass
   *
   * @return value of listenerClass
   */
  public String getListenerClass() {
    return listenerClass;
  }

  /**
   * @param listenerClass The listenerClass to set
   */
  public void setListenerClass( String listenerClass ) {
    this.listenerClass = listenerClass;
  }

  /**
   * Gets listenerMethod
   *
   * @return value of listenerMethodf
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

  /**
   * Gets extraWidth
   *
   * @return value of extraWidth
   */
  public int getExtraWidth() {
    return extraWidth;
  }

  /**
   * @param extraWidth The extraWidth to set
   */
  public void setExtraWidth( int extraWidth ) {
    this.extraWidth = extraWidth;
  }

  /**
   * Gets alignRight
   *
   * @return value of alignRight
   */
  public boolean isAlignRight() {
    return alignRight;
  }

  /**
   * @param alignRight The alignRight to set
   */
  public void setAlignRight( boolean alignRight ) {
    this.alignRight = alignRight;
  }
}
