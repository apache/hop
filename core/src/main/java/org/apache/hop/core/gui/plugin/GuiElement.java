package org.apache.hop.core.gui.plugin;

/**
 * This annotation allows a method in a GuiPlugin to be identified as a contributor to the Hop UI
 *
 */
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
   * The ID of the GUI element ID after which this element will be inserted
   * @return
   */
  public String previousId() default "";

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
}
