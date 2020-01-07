package org.apache.hop.metastore.persist;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface MetaStoreElementType {

  String name();

  String description() default "";

  String dialogClassname() default "";
}
