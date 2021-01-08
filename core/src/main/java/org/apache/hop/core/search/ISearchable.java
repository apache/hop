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

package org.apache.hop.core.search;

/**
 * The searchable object is an object which can be examined by a search engine.
 *
 */
public interface ISearchable<T> {
  /**
   * Where the searchable is
   * @return
   */
  String getLocation();

  /**
   * What it is called
   * @return
   */
  String getName();

  /**
   * @return The type of searchable: pipeline, workflow, type of metadata object, ...
   */
  String getType();

  /**
   * What the filename is (if there is any)
   * @return
   */
  String getFilename();

  /**
   * @return The object to search itself
   */
  T getSearchableObject();

  /**
   * In case you want to have a callback for this searchable.
   * This can be used to open or locate the searchable object.
   *
   * @return The callback
   */
  ISearchableCallback getSearchCallback();
}
