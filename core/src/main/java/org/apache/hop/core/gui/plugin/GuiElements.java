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

package org.apache.hop.core.gui.plugin;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This represents a list of GUI elements under a certain heading or ID
 */
public class GuiElements extends BaseGuiElements implements Comparable<GuiElements> {

  private String id;

  private String order;

  private String parentId;

  private String label;

  private String toolTip;

  private GuiElementType type;

  private String image;
  private String disabledImage;

  private boolean variablesEnabled;

  private boolean password;

  private String fieldName;

  private Class<?> fieldClass;

  private String getterMethod;

  private String setterMethod;

  private String getComboValuesMethod;

  private List<GuiElements> children;

  private boolean ignored;

  private boolean addingSeparator;

  private ClassLoader classLoader;


  // The singleton listener class to use
  private boolean singleTon;
  private Class<?> listenerClass;
  private String listenerMethod;

  public GuiElements() {
    children = new ArrayList<>();
  }

  public GuiElements( GuiWidgetElement guiElement, Field field ) {
    this();

    String fieldName = field.getName();
    Class<?> fieldClass = field.getType();
    String fieldPackageName = field.getDeclaringClass().getPackage().getName();
    
    if (StringUtil.isEmpty( guiElement.id() )) {
      this.id = field.getName();
    } else {
      this.id = guiElement.id();
    }
    this.order = guiElement.order();
    this.type = guiElement.type();
    this.parentId = guiElement.parentId();
    this.fieldName = fieldName;
    this.fieldClass = fieldClass;
    this.getterMethod = calculateGetterMethod( guiElement, fieldName );
    this.setterMethod = calculateSetterMethod( guiElement, fieldName );
    this.getComboValuesMethod = guiElement.comboValuesMethod();
    this.image = guiElement.image();
    this.disabledImage = null;
    this.variablesEnabled = guiElement.variables();
    this.password = guiElement.password();
    this.ignored = guiElement.ignored();
    this.addingSeparator = guiElement.separator();
    this.label = getTranslation( guiElement.label(), fieldPackageName, field.getDeclaringClass() );
    this.toolTip = getTranslation( guiElement.toolTip(), fieldPackageName, field.getDeclaringClass() );
  }


  /**
   * Sort the children using the sort order.
   * If no sort field is available we use the ID
   */
  public void sortChildren() {
    Collections.sort( children );
  }

  public GuiElements findChild( String id ) {
    for ( GuiElements child : children ) {
      if ( child.getId() != null && child.getId().equals( id ) ) {
        return child;
      }
    }
    return null;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    GuiElements that = (GuiElements) o;
    return id.equals( that.id );
  }

  @Override public int hashCode() {
    return Objects.hash( id );
  }

  @Override public int compareTo( GuiElements e ) {
    if (StringUtils.isNotEmpty( order ) && StringUtils.isNotEmpty( e.id )) {
      return order.compareTo( e.order );
    } else {
      return id.compareTo( e.id );
    }
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
   * Gets order
   *
   * @return value of order
   */
  public String getOrder() {
    return order;
  }

  /**
   * @param order The order to set
   */
  public void setOrder( String order ) {
    this.order = order;
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
  public GuiElementType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( GuiElementType type ) {
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
   * Gets children
   *
   * @return value of children
   */
  public List<GuiElements> getChildren() {
    return children;
  }

  /**
   * @param children The children to set
   */
  public void setChildren( List<GuiElements> children ) {
    this.children = children;
  }

  /**
   * Gets variablesEnabled
   *
   * @return value of variablesEnabled
   */
  public boolean isVariablesEnabled() {
    return variablesEnabled;
  }

  /**
   * @param variablesEnabled The variablesEnabled to set
   */
  public void setVariablesEnabled( boolean variablesEnabled ) {
    this.variablesEnabled = variablesEnabled;
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
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * Gets getterMethod
   *
   * @return value of getterMethod
   */
  public String getGetterMethod() {
    return getterMethod;
  }

  /**
   * @param getterMethod The getterMethod to set
   */
  public void setGetterMethod( String getterMethod ) {
    this.getterMethod = getterMethod;
  }

  /**
   * Gets setterMethod
   *
   * @return value of setterMethod
   */
  public String getSetterMethod() {
    return setterMethod;
  }

  /**
   * @param setterMethod The setterMethod to set
   */
  public void setSetterMethod( String setterMethod ) {
    this.setterMethod = setterMethod;
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
   * Gets fieldClass
   *
   * @return value of fieldClass
   */
  public Class<?> getFieldClass() {
    return fieldClass;
  }

  /**
   * @param fieldClass The fieldClass to set
   */
  public void setFieldClass( Class<?> fieldClass ) {
    this.fieldClass = fieldClass;
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
   * Gets listenerClass
   *
   * @return value of listenerClass
   */
  public Class<?> getListenerClass() {
    return listenerClass;
  }

  /**
   * @param listenerClass The listenerClass to set
   */
  public void setListenerClass( Class<?> listenerClass ) {
    this.listenerClass = listenerClass;
  }

  /**
   * Gets menuMethod
   *
   * @return value of menuMethod
   */
  public String getListenerMethod() {
    return listenerMethod;
  }

  /**
   * @param listenerMethod The menuMethod to set
   */
  public void setListenerMethod( String listenerMethod ) {
    this.listenerMethod = listenerMethod;
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


}
