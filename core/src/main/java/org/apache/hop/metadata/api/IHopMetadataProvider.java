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

import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;

import java.util.List;

public interface IHopMetadataProvider {

  String getDescription();

  <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer( Class<T> managedClass ) throws HopException;

  /**
   * @return The password encoder/decoder used in the serializers for password encoding
   */
  ITwoWayPasswordEncoder getTwoWayPasswordEncoder();

  /**
   * Get a list of all the available metadata classes on this system.
   * It's a convenience.  You can also get this information through the PluginRegistry.
   *
   * @param <T>
   * @return The list of all available classes including those who have no objects defined.
   * @throws HopException
   */
  <T extends IHopMetadata> List<Class<T>> getMetadataClasses();


  /**
   * Find the class corresponding to the key/id of the metadata plugin
   *
   * @param key The key of the metadata object class
   * @return The class for the given key
   * @throws HopException
   */
  <T extends IHopMetadata> Class<T> getMetadataClassForKey( String key ) throws HopException;
}
