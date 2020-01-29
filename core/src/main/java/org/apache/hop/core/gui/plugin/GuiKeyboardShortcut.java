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
public @interface GuiKeyboardShortcut {
  boolean control() default false;
  boolean alt() default false;
  boolean shift() default false;
  int key() default -1;
}
