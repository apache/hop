package org.apache.hop.core.action;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation indicated that the annotated method handles an action in a certain context (the current class)
 * The method is part of a IGuiContext class painted with a @GuiContextPlugin annotation.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.METHOD  )
public @interface GuiContextAction {

  /** The ID of the action that can be handled
   *
   * @return The ID of the action to be handled.
   */
  String id();
}
