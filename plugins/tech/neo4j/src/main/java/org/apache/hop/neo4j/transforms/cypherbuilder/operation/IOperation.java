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
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.neo4j.transforms.cypherbuilder.Parameter;

@HopMetadataObject(objectFactory = OperationFactory.class, xmlKey = "operationType")
public interface IOperation extends Cloneable {

  /** The name and type uniquely identifies an operation. */
  String getName();

  /**
   * @param name The new name of the operation
   */
  void setName(String name);

  /** The type of operation */
  OperationType getOperationType();

  /**
   * Generate the relevant Cypher clause.
   *
   * @param unwindAlias If you want the cypher in an UNWIND format, set this row alias.
   * @param parameters The available parameters.
   * @throws HopException
   */
  String getCypherClause(String unwindAlias, List<Parameter> parameters) throws HopException;

  /**
   * @return true if the operation requires a "write transaction" to execute.
   */
  boolean needsWriteTransaction();

  /** Make a copy of the operation */
  IOperation clone();
}
