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
 *
 */

package org.apache.hop.rest.v1.resources.location;

public class ListExecutionsRequest {
  /** Set this to true if you want to see child executions of workflows and pipelines */
  private boolean includeChildren;

  /** the maximum number of IDs to retrieve or a value <=0 to get all IDs */
  private int limit;

  public ListExecutionsRequest() {
    includeChildren = false;
    limit = 100;
  }

  public ListExecutionsRequest(boolean includeChildren, int limit) {
    this.includeChildren = includeChildren;
    this.limit = limit;
  }

  /**
   * Gets includeChildren
   *
   * @return value of includeChildren
   */
  public boolean isIncludeChildren() {
    return includeChildren;
  }

  /**
   * Sets includeChildren
   *
   * @param includeChildren value of includeChildren
   */
  public void setIncludeChildren(boolean includeChildren) {
    this.includeChildren = includeChildren;
  }

  /**
   * Gets limit
   *
   * @return value of limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Sets limit
   *
   * @param limit value of limit
   */
  public void setLimit(int limit) {
    this.limit = limit;
  }
}
