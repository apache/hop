/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.model.validation;

public class NodeProperty {
  private String nodeName;
  private String propertyName;

  public NodeProperty() {}

  public NodeProperty(String nodeName, String propertyName) {
    this();
    this.nodeName = nodeName;
    this.propertyName = propertyName;
  }

  /**
   * Gets nodeName
   *
   * @return value of nodeName
   */
  public String getNodeName() {
    return nodeName;
  }

  /** @param nodeName The nodeName to set */
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Gets propertyName
   *
   * @return value of propertyName
   */
  public String getPropertyName() {
    return propertyName;
  }

  /** @param propertyName The propertyName to set */
  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }
}
