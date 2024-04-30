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
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.transforms.cypherbuilder.Parameter;

public class DeleteOperation extends BaseOperation {
  public DeleteOperation() {
    super(OperationType.DELETE);
  }

  /** Use "Detach delete? */
  @HopMetadataProperty private boolean detach;

  @Override
  public String getCypherClause(String unwindList, List<Parameter> parameters) throws HopException {
    StringBuilder cypher = new StringBuilder();
    if (detach) {
      cypher.append("DETACH ");
    }
    cypher.append(operationType.keyWord());

    return cypher.toString();
  }

  @Override
  public boolean needsWriteTransaction() {
    return true;
  }

  /**
   * Gets detach
   *
   * @return value of detach
   */
  public boolean isDetach() {
    return detach;
  }

  /**
   * Sets detach
   *
   * @param detach value of detach
   */
  public void setDetach(boolean detach) {
    this.detach = detach;
  }
}
