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
 * An alternate way of defining transforms. Classes annotated with "Transform" are automatically
 * recognized and registered as a transform.
 *
 * <p>Important: The XML definitions alienate annotated transforms and the two methods of definition
 * are therefore mutually exclusive.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Transform {

  /**
   * @return The ID of the transform. You can specify more than one ID in a comma separated format:
   *     id1,id2,id3 for deprecation purposes.
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

  /**
   * @return True if the JDBC drivers have to be loaded for this transform
   */
  boolean isIncludeJdbcDrivers() default false;

  /** an Array of ActionTransformTypes for this transform */
  ActionTransformType[] actionTransformTypes() default {};

  /**
   * Allow-list of pipeline engine plugin ids this transform is supported on. Empty (the default)
   * means the transform expresses no opinion — engines may still accept or reject it via their own
   * {@code IPipelineEngine.supports()} verdict. A trailing wildcard is supported: {@code "Beam*"}
   * matches every engine id starting with {@code "Beam"}, {@code "*"} matches every engine. The
   * full id ({@code "BeamDirectPipelineEngine"}, {@code "Local"}, …) is the engine plugin's
   * {@code @PipelineEnginePlugin.id} value. If both {@link #supportedEngines()} and {@link
   * #excludedEngines()} are non-empty, registration fails — pick one form per transform.
   */
  String[] supportedEngines() default {};

  /**
   * Deny-list counterpart to {@link #supportedEngines()}. A transform that lists an engine here is
   * treated as UNSUPPORTED on that engine in the UI and run dialog, regardless of the engine's own
   * verdict. Empty (the default) means no exclusions; same wildcard rules apply.
   */
  String[] excludedEngines() default {};
}
