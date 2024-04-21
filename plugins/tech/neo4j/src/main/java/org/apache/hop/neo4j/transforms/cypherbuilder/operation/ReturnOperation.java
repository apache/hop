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

package org.apache.hop.neo4j.transforms.cypherbuilder.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.transforms.cypherbuilder.Parameter;
import org.apache.hop.neo4j.transforms.cypherbuilder.ReturnValue;

public class ReturnOperation implements IOperation {

  @HopMetadataProperty private String name;

  @HopMetadataProperty private OperationType operationType;

  @HopMetadataProperty private List<ReturnValue> returnValues;

  public ReturnOperation() {
    this.operationType = OperationType.RETURN;
    this.returnValues = new ArrayList<>();
  }

  public ReturnOperation(String name, List<ReturnValue> returnValues) {
    this.name = name;
    this.returnValues = returnValues;
  }

  public ReturnOperation(ReturnOperation o) {
    this();
    this.name = o.name;
    o.returnValues.forEach(v -> this.returnValues.add(new ReturnValue(v)));
  }

  public ReturnOperation clone() {
    return new ReturnOperation(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReturnOperation that = (ReturnOperation) o;
    if (this.operationType != that.operationType) {
      return false;
    }
    if (this.returnValues.size() != that.returnValues.size()) {
      return false;
    }
    for (int i = 0; i < this.returnValues.size(); i++) {
      if (!this.returnValues.get(i).equals(that.returnValues.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(operationType, returnValues);
  }

  @Override
  public String getCypherClause(String unwindAlias, List<Parameter> parameters)
      throws HopException {
    // RETURN
    StringBuilder cypher = new StringBuilder(operationType.keyWord());
    // n.value1 AS Value1, o.value2 AS Value2
    for (int r = 0; r < returnValues.size(); r++) {
      ReturnValue returnValue = returnValues.get(r);
      if (r > 0) {
        cypher.append(",");
      }
      cypher.append(" ").append(returnValue.getCypherClause());
    }
    cypher.append(" ");

    return cypher.toString();
  }

  @Override
  public boolean needsWriteTransaction() {
    return false;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets operationType
   *
   * @return value of operationType
   */
  @Override
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * Sets operationType
   *
   * @param operationType value of operationType
   */
  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }

  /**
   * Gets returnValues
   *
   * @return value of returnValues
   */
  public List<ReturnValue> getReturnValues() {
    return returnValues;
  }

  /**
   * Sets returnValues
   *
   * @param returnValues value of returnValues
   */
  public void setReturnValues(List<ReturnValue> returnValues) {
    this.returnValues = returnValues;
  }
}
