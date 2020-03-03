package org.apache.hop.core.gui.plugin;

/**
 * This annotation allows a method in a GuiPlugin to be identified as a contributor to the Hop UI
 */

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( { ElementType.FIELD, ElementType.METHOD} )
public @interface GuiAction {
  /**
   * @return The name or short description of the action
   */
  String name();

  /**
   * @return The long description of the action
   */
  String tooltip() default "";

  /**
   * @return The iconic representation of the action
   */
  String image() default "ui/images/TRN.svg";
}
