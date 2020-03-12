package org.apache.hop.core.action;

import org.apache.hop.core.gui.plugin.GuiActionType;

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

  String id();

  String parentId();

  GuiActionType type();

  String name();

  String tooltip();

  String image();

  String[] keywords() default {};
}
