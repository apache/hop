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

package org.apache.hop.metadata.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface HopMetadata {

  /**
   * The key or entry for this hop metadata object type. It's going to determine the name of the
   * folder, node key, ...
   *
   * @return The key for these Hop metadata objects
   */
  String key();

  /**
   * The key for this hop metadata object. It will be translated into a sub-folder for the JSON
   * serializer
   *
   * @return The key of the hop metadata object class which carries this annotation
   */
  String name();

  String description() default "";

  String image() default "ui/images/folder.svg";

  String documentationUrl() default "";

  /**
   * A HopMetadataPropertyType provides information about the purpose of a HopMetadataProperty.
   *
   * @return the type of metadata this property represents.
   */
  HopMetadataPropertyType hopMetadataPropertyType() default HopMetadataPropertyType.NONE;
}
