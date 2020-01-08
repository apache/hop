package org.apache.hop.metastore.persist;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention( RetentionPolicy.RUNTIME )
@Target( { ElementType.FIELD } )
public @interface MetaStoreAttribute {

  /**
   * @return The key of the attribute
   */
  String key() default "";

  /**
   * @return true if you want this string to be encoded and decoded using the metastore two way password encoder.
   */
  boolean password() default false;

  /**
   * @return true if this attribute is a reference to an object which needs to be resolved by name from a list.  The list is provided to the factory.  The list has a name specified with nameListKey().
   */
  boolean nameReference() default false;

  /**
   * @return If this attribute is a reference to an object which needs to be resolved by name from a list, this provides the name of the list provided to the factory.
   */
  String nameListKey() default "";

  /**
   * @return true if this attribute is a reference to an object which needs to be resolved by filename from a list.  The list is provided to the factory.  The list has a name specified with
   * filenameListKey().
   */
  boolean filenameReference() default false;

  /**
   * @return If this attribute is a reference to an object which needs to be resolved by filename from a list, this provides the name of the list provided to the factory.
   */
  String filenameListKey() default "";

  /**
   * @return true if you want to have the attribute to be persisted (loaded/saved) through the factory provided by the factoryNameKey() method.
   */
  boolean factoryNameReference() default false;

  /**
   * @return the key/reference of the factory provided to the factory resolving this attribute.
   */
  String factoryNameKey() default "";

  /**
   * @return the name of the shared flag indicator.  This is the name of the method in the referenced object which indicates whether or not the object should be updated/referenced centrally (true)
   * or embedded locally (false).
   * As the method name implies, this ONLY works with the use of a factory, not through a simple name or file reference.
   */
  String factorySharedIndicatorName() default "";
}
