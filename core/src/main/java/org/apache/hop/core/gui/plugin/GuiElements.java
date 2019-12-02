package org.apache.hop.core.gui.plugin;

import org.apache.hop.core.gui.PrimitiveGCInterface;

import java.util.List;
import java.util.Objects;

/**
 * This represents a list of GUI elements under a certain heading or ID
 */
public class GuiElements {

  private String id;

  private String label;

  private GuiElementType type;

  private PrimitiveGCInterface.EImage image;

  private boolean variablesEnabled;

  private boolean password;

  private String getterMethod;

  private String setterMethod;

  private String getComboValuesMethod;

  private List<GuiElements> children;

  public GuiElements() {
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
  public PrimitiveGCInterface.EImage getImage() {
    return image;
  }

  /**
   * @param image The image to set
   */
  public void setImage( PrimitiveGCInterface.EImage image ) {
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
}
