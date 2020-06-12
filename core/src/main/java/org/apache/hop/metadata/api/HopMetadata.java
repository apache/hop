package org.apache.hop.metadata.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention( RetentionPolicy.RUNTIME )
public @interface HopMetadata {

  /**
   * The key or entry for this hop metadata object type.  It's going to determine the name of the folder, node key, ...
   * @return The key for these Hop metadata objects
   */
  String key();

  /**
   * The key for this hop metadata object.  It will be translated into a sub-folder for the JSON serializer
   *
   * @return The key of the hop metadata object class which carries this annotation
   */
  String name();

  String description() default "";

  String iconImage() default "ui/images/folder.svg";
}
