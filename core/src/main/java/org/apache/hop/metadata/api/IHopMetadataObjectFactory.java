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

/**
 * This describes a standard interface for instantiating metadata objects and providing the unique identifier for serialization.
 *
 */
public interface IHopMetadataObjectFactory {

  /**
   * Given an ID (usually a plugin ID), instantiate the appropriate object for it.
   * @param id The ID to use to create the object
   * @param parentObject The optional parent object if certain things like variables need to be inherited from it.
   * @return The instantiated object
   */
  Object createObject( String id, Object parentObject ) throws HopException;

  /**
   * get the ID for the given object.  Usually this is the plugin ID.
   * @param object The object to get the ID for
   * @return The object ID
   * @throws HopException
   */
  String getObjectId(Object object) throws HopException;
}
