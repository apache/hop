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

package org.apache.hop.neo4j.transforms.gencsv;

import org.apache.hop.neo4j.core.data.GraphPropertyDataType;

import java.util.Objects;

public class IdType {

  private String id;
  private GraphPropertyDataType type;

  public IdType() {}

  public IdType(String id, GraphPropertyDataType type) {
    this.id = id;
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IdType idType = (IdType) o;
    return id.equals(idType.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public GraphPropertyDataType getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(GraphPropertyDataType type) {
    this.type = type;
  }
}
