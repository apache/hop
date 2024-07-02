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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hop.core.injection.DefaultInjectionTypeConverter;
import org.apache.hop.core.injection.InjectionTypeConverter;

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

  /**
   * For enums: store and retrieve this value not with the name of the enum but with a "code"
   * property. Make sure your enum implements {@link IEnumHasCode} and has method getCode() to
   * recognize it.
   *
   * @return True if you want to store the value of an enum by its code instead its name.
   */
  boolean storeWithCode() default false;

  /**
   * @return The group key. In case this is a list use this key to encapsulate the list/array.
   */
  String groupKey() default "";

  /**
   * @return The default value to return for a non-existing boolean value
   */
  boolean defaultBoolean() default false;

  /** What is the name of the enum to pick in case the code in the metadata can't be found? * */
  String enumNameWhenNotFound() default "";

  /**
   * @return Prevents the item to be considered in injection. Default value: false
   */
  boolean isExcludedFromInjection() default false;

  /**
   * @return The metadata key for this property. Don't specify any key if you want this to be the
   *     same as key();
   */
  String injectionKey() default "";

  /**
   * @return The metadata description for this property. (i18n)
   */
  String injectionKeyDescription() default "";

  /**
   * @return The metadata group key to which this property belongs. Don't specify any key if you
   *     want this to be the same as key();
   */
  String injectionGroupKey() default "";

  /**
   * @return A description of the metadata group key to which this property belongs. (i18n)
   */
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

  /**
   * @return true to store metadata inline with the parent metadata, not in a sub-element.
   */
  boolean inline() default false;

  /**
   * Reads old format XML where a list of values is stored inline. XML like the following needs to
   * be turned into 3 KeyValue pairs:
   *
   * <p>{@code <parent> <key>k1</key><value>v1</value> <key>k2</key><value>v2</value>
   * <key>k3</key><value>v3</value> </parent> }
   *
   * <p>In this scenario we would specify the tags "key" and "value" to populate the list correctly.
   *
   * @return
   */
  String[] inlineListTags() default {};

  /**
   * For metadata injection: if you want to convert an integer to a code (and vice-versa)
   *
   * @return The integer-to-string converter
   */
  Class<? extends IIntCodeConverter> intCodeConverter() default IIntCodeConverter.None.class;

  /**
   * For metadata injection: if you want to convert a String (XML, JSON, ...) to an object
   *
   * @return The string-to-object converter
   */
  Class<? extends IStringObjectConverter> injectionStringObjectConverter() default
      IStringObjectConverter.None.class;

  /**
   * A HopMetadataPropertyType provides information about the purpose of a HopMetadataProperty.
   *
   * @return the type of metadata this property represents.
   */
  HopMetadataPropertyType hopMetadataPropertyType() default HopMetadataPropertyType.NONE;
}
