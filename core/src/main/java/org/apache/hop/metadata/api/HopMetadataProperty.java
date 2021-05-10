/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.metadata.api;

import org.apache.hop.core.injection.DefaultInjectionTypeConverter;
import org.apache.hop.core.injection.InjectionTypeConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** A field which is painted with this annotation is picked up by the Hop Metadata serializers */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface HopMetadataProperty {

  /**
   * The optional key to store this metadata property under. By the default the name of the field is
   * taken.
   *
   * @return
   */
  String key() default "";

  /**
   * @return Set to true if you want this String field to be stored as a password: encoded or
   *     obfuscated
   */
  boolean password() default false;

  /**
   * @return true if this field should be stored as a name reference because it is a HopMetadata
   *     class
   */
  boolean storeWithName() default false;

  /** @return The group key. In case this is a list use this key to encapsulate the list/array. */
  String groupKey() default "";

  /** @return The default value to return for a non-existing boolean value */
  boolean defaultBoolean() default false;

  /**
   * @return The metadata key for this property. Don't specify any key if you want this to be the
   *     same as key();
   */
  String injectionKey() default "";

  /** @return The metadata description for this property. (i18n) */
  String injectionKeyDescription() default "";

  /**
   * @return The metadata group key to which this property belongs. Don't specify any key if you
   *     want this to be the same as key();
   */
  String injectionGroupKey() default "";

  /** @return A description of the metadata group key to which this property belongs. (i18n) */
  String injectionGroupDescription() default "";

  /**
   * A description of the field. Right now this is used only for metadata injection purposes
   *
   * @return The description of the property
   */
  String description() default "";

  /**
   * @return The class to instantiate to convert metadata properly for this property (dates,
   *     numbers, ...)
   */
  Class<? extends InjectionTypeConverter> injectionConverter() default
      DefaultInjectionTypeConverter.class;
}
