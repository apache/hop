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

package org.apache.hop.neo4j.transforms.cypher;

import org.apache.hop.core.injection.Injection;

public class ReturnValue {

  @Injection(name = "RETURN_NAME", group = "RETURNS")
  private String name;

  @Injection(name = "RETURN_TYPE", group = "RETURNS")
  private String type;

  @Injection(name = "RETURN_SOURCE_TYPE", group = "RETURNS")
  private String sourceType;

  public ReturnValue() {}

  public ReturnValue(String name, String type, String sourceType) {
    this.name = name;
    this.type = type;
    this.sourceType = sourceType;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets sourceType
   *
   * @return value of sourceType
   */
  public String getSourceType() {
    return sourceType;
  }

  /** @param sourceType The sourceType to set */
  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }
}
