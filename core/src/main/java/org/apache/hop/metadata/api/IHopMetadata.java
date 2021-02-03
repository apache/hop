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

/**
 * Hop Metadata object classes should implement this interface so the plugin system can recognize
 * them. Other than that they just have a name really. Maybe later we can add keywords and some
 * ACLs.
 */
public interface IHopMetadata {
  /**
   * Get the name of the metadata object.
   *
   * @return The name uniquely identifying the metadata object
   */
  String getName();

  /**
   * Set the name.
   *
   * @param name The name uniquely identifying the metadata object
   */
  void setName(String name);

  /**
   * Get the source of the metadata object. Plugins can use this to mix metadata from various
   * sources. It helps to figure out where this object originated.
   *
   * @return The source of metadata or null if it's not specified.
   */
  String getMetadataProviderName();

  /**
   * Set the source of the metadata. Plugins can use this to mix metadata from various sources.
   * It helps to figure out where this object originated.
   *
   * @param metadataProviderName The source of metadata or null if it's not specified
   */
  void setMetadataProviderName( String metadataProviderName );
}
