package org.apache.hop.core.gui.plugin.tab;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface GuiTab {

    String type() default "";
    String id() default "";
    String parentId() default "";
    String description() default "";
    String targetClass() default "";
    int tabPosition() default 0;

}
