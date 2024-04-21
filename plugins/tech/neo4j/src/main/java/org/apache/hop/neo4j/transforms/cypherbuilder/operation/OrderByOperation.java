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

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.neo4j.transforms.cypherbuilder.Parameter;
import org.apache.hop.neo4j.transforms.cypherbuilder.Property;

public class OrderByOperation extends BaseOperation {

  public OrderByOperation() {
    super(OperationType.ORDER_BY);
  }

  public OrderByOperation(OrderByOperation o) {
    super(o);
  }

  @Override
  public String getCypherClause(String unwindAlias, List<Parameter> parameters)
      throws HopException {
    // ORDER BY
    //
    StringBuilder cypher = new StringBuilder(operationType.keyWord());
    cypher.append(" ");

    // n.name DESC, upper(n.lastName), ...
    //
    for (int i = 0; i < properties.size(); i++) {
      Property property = properties.get(i);
      if (i > 0) {
        cypher.append(", ");
      }
      cypher.append(property.getOrderByCypherClause());
    }
    cypher.append(" ");
    return cypher.toString();
  }

  @Override
  public boolean needsWriteTransaction() {
    return false;
  }

  @Override
  public OrderByOperation clone() {
    return new OrderByOperation(this);
  }
}
