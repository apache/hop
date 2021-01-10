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

import org.apache.hop.core.exception.HopException;

public interface ISearchableCallback {
  /**
   * When a search result is selected, call this method.
   * It can be used to open the searchable in the GUI
   *
   * @param searchable
   * @param searchResult
   * @throws HopException
   */
  public void callback(ISearchable searchable, ISearchResult searchResult) throws HopException;
}
