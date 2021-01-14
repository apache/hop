/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An alternate way of defining transforms. Classes annotated with "Transform" are automatically recognized and registered as a
 * transform.
 * <p>
 * Important: The XML definitions alienate annotated transforms and the two methods of definition are therefore mutually
 * exclusive.
 *
 * @author Alex Silva
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface Transform {

  /**
   * @return The ID of the transform. You can specify more than one ID in a comma separated format: id1,id2,id3 for
   * deprecation purposes.
   */
  String id();

  String name();

  String description() default "";

  /**
   * @return The image resource path
   */
  String image() default "";

  /**
   * @return True if a separate class loader is needed every time this class is instantiated
   */
  boolean isSeparateClassLoaderNeeded() default false;

  String classLoaderGroup() default "";

  String categoryDescription() default "";

  /**
   * This is a deprecated method: please use the i18n:package:key format in name, description and categoryDescription
   *
   * @return
   */
  @Deprecated
  String i18nPackageName() default "";

  /**
   * @return The documentation url
   */
  String documentationUrl() default "";

  /**
   * @return The cases url
   */
  String casesUrl() default "";

  /**
   * @return The forum url
   */
  String forumUrl() default "";

  String suggestion() default "";

  String[] keywords() default {};
}
