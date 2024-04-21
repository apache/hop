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

public class MergeOperation extends BaseOperation {

  public MergeOperation() {
    super(OperationType.MERGE);
  }

  public MergeOperation(MergeOperation o) {
    super(o);
  }

  @Override
  public MergeOperation clone() {
    return new MergeOperation(this);
  }

  @Override
  public String getCypherClause(String unwindAlias, List<Parameter> parameters)
      throws HopException {
    // MERGE
    StringBuilder cypher = new StringBuilder(operationType.keyWord());
    // (n:Movie
    cypher.append("(").append(getLabelsClause());
    // { id : {pId} }
    cypher.append(getKeysClause(unwindAlias)).append(") ");
    // SET prop1={param1}, prop2={param2}
    cypher.append(getSetClause(unwindAlias));
    return cypher.toString();
  }

  @Override
  public boolean needsWriteTransaction() {
    return true;
  }
}
