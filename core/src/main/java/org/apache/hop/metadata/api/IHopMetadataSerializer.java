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

import org.apache.hop.core.exception.HopException;

import java.util.List;

/**
 * This metadata interface describes how an object T can be serialized and analyzed.
 *
 * @param <T>
 */
public interface IHopMetadataSerializer<T extends IHopMetadata> {

  /**
   * @return A description of the serialized metadata (type)
   */
  String getDescription();

  /**
   * Load (de-serialize) an object with the given name
   *
   * @param objectName
   * @return The object with the specified name
   * @throws HopException
   */
  T load( String objectName ) throws HopException;

  /**
   * Save (serialize) the provided object.  If an object with the same name exists, it is updated, otherwise it's created.
   *
   * @param object
   * @throws HopException
   */
  void save( T object ) throws HopException;

  /**
   * Delete the object with the given name.
   *
   * @param name The name of the object to delete
   * @return The deleted object
   * @throws HopException
   */
  T delete( String name ) throws HopException;

  /**
   * List all the object names
   *
   * @return A list of object names
   * @throws HopException
   */
  List<String> listObjectNames() throws HopException;

  /**
   * See if an object with the given name exists.
   *
   * @param name The name of the object to check
   * @return true if an object with the given name exists.
   * @throws HopException
   */
  boolean exists( String name ) throws HopException;

  /**
   * @return The class of the objects which are saved and loaded.
   */
  Class<T> getManagedClass();

  /**
   * The metadata provider allowing you to serialize other objects.
   *
   * @return The metadata provider of this serializer
   */
  IHopMetadataProvider getMetadataProvider();

  /**
   * Load all available objects
   *
   * @return A list of all available objects
   */
  List<T> loadAll() throws HopException;
}
