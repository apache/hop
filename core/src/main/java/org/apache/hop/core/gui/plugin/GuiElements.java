package org.apache.hop.core.gui.plugin;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * This represents a list of GUI elements under a certain heading or ID
 */
public class GuiElements {

  private String id;

  private String parentId;

  private String label;

  private String toolTip;

  private String i18nPackage;

  private GuiElementType type;

  private String image;

  private boolean variablesEnabled;

  private boolean password;

  private String fieldName;

  private Class<?> fieldClass;

  private String getterMethod;

  private String setterMethod;

  private String getComboValuesMethod;

  private List<GuiElements> children;

  private String order;

  private boolean ignored;


  public GuiElements() {
    children = new ArrayList<>();
  }

  public GuiElements( GuiElement guiElement, String fieldName, Class<?> fieldClass ) {
    this();
    this.id = guiElement.id();
    this.type = guiElement.type();
    this.parentId = guiElement.parentId();
    this.order = guiElement.order();
    this.fieldName = fieldName;
    this.fieldClass = fieldClass;
    this.getterMethod = calculateGetterMethod( guiElement, fieldName );
    this.setterMethod = calculateSetterMethod( guiElement, fieldName );
    this.getComboValuesMethod = guiElement.comboValuesMethod();
    this.image = guiElement.image();
    this.variablesEnabled = guiElement.variables();
    this.password = guiElement.password();
    this.i18nPackage = guiElement.i18nPackage();
    this.ignored = guiElement.ignored();
    if (StringUtils.isNotEmpty(i18nPackage)) {
      this.label = BaseMessages.getString( i18nPackage, guiElement.label() );
      this.toolTip = BaseMessages.getString( i18nPackage, guiElement.toolTip() );
    } else {
      this.label = guiElement.label();
      this.toolTip = guiElement.toolTip();
    }
  }

  private String calculateGetterMethod( GuiElement guiElement, String fieldName ) {
    if ( StringUtils.isNotEmpty( guiElement.getterMethod() ) ) {
      return guiElement.getterMethod();
    }
    String getter = "get" + StringUtil.initCap( fieldName );
    return getter;
  }


  private String calculateSetterMethod( GuiElement guiElement, String fieldName ) {
    if ( StringUtils.isNotEmpty( guiElement.setterMethod() ) ) {
      return guiElement.setterMethod();
    }
    String getter = "set" + StringUtil.initCap( fieldName );
    return getter;
  }

  /**
   * Sort the children using the sort order.
   * If no sort field is available we use the ID
   */
  public void sortChildren() {
    Collections.sort( children, new Comparator<GuiElements>() {
      @Override public int compare( GuiElements o1, GuiElements o2 ) {
        if (StringUtils.isNotEmpty( o1.order ) && StringUtils.isNotEmpty( o2.order )) {
          return o1.order.compareTo( o2.order );
        } else {
          return o1.id.compareTo( o2.id );
        }
      }
    } );
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
   * Gets i18nPackage
   *
   * @return value of i18nPackage
   */
  public String getI18nPackage() {
    return i18nPackage;
  }

  /**
   * @param i18nPackage The i18nPackage to set
   */
  public void setI18nPackage( String i18nPackage ) {
    this.i18nPackage = i18nPackage;
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
}
