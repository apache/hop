package org.apache.hop.core.gui.plugin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation allows a method in a GuiPlugin to be identified as a contributor to the Hop UI
 *
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.FIELD )
public @interface GuiElement {

  /**
   * Every GUI Element has a unique ID so it can be replaced by other plugins at any given time.
   *
   * @return The unique ID of the GUI Element
   */
  public String id();

  /**
   * The type of GUI Element this method covers for
   *
   * @return
   */
  public GuiElementType type();

  /**
   * The label of the GUI element: the menu item text and so on.
   * @return The GUI Element Label
   */
  public String label() default "";

  /**
   * The tooltip of the GUI element (when applicable)
   * @return The GUI Element tooltip for the widget and the label
   */
  public String toolTip() default "";

  /**
   * The name of the i18n package for the label and tooltip
   */
  public String i18nPackage() default "";

  /**
   * The image filename of the GUI Element, usually an SVG icon.
   * @return The
   */
  public String image() default "";

  /**
   * The ID of the parent GUI element. This is usually the parent menu or toolbar, ...
   * @return The ID of the parent GUI element
   */
  public String parentId() default "";

  /**
   * @return True if the text element you define is a password with an asterisk mask
   */
  public boolean password() default false;

  /**
   * @return true if the widget supports variables
   */
  public boolean variables() default true;

  /**
   * @return The getter method of the property if it's non-standard
   */
  public String getterMethod() default "";

  /**
   * @return The setter method of the property if it's non-standard
   */
  public String setterMethod() default "";

  /**
   * @return The method which returns a String[] to populate a combo box widget GUI element
   */
  public String comboValuesMethod() default "";

  /**
   * You can use this to order the GUI Elements for a given scenario
   * @return The value on which the system will sort alphabetically
   */
  public String order() default "";

  /**
   * Set this flag to true if you want to ignore the field as a GUI Element.
   * You can use this to override a GUI element from a base class.
   * @return
   */
  public boolean ignored() default false;
}
