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

import org.neo4j.driver.Record;

public class ConstraintDetails {
  private String name;
  private String description;

  public ConstraintDetails() {}

  public ConstraintDetails(String name, String description) {
    this();
    this.name = name;
    this.description = description;
  }

  public ConstraintDetails(Record record) {
    this();
    this.name = record.get("name").asString();
    this.description = record.get("description").asString();
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
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /** @param description The description to set */
  public void setDescription(String description) {
    this.description = description;
  }
}
