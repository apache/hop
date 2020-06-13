package org.apache.hop.metadata.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A field which is painted with this annotation is picked up by the Hop Metadata serializers
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( { ElementType.FIELD } )
public @interface HopMetadataProperty {

  /**
   * The optional key to store this metadata property under.
   * By the default the name of the field is taken.
   *
   * @return
   */
  String key() default "";

  /**
   * @return Set to true if you want this String field to be stored as a password: encoded or obfuscated
   */
  boolean password() default false;

  /**
   * @return true if this field should be stored as a name reference because it is a HopMetadata class
   */
  boolean storeWithName() default false;
}
