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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;

public class OperationFactory implements IHopMetadataObjectFactory {
  public static IOperation createOperation(OperationType operationType) {
    assert operationType != null : "Please specify an operation type";
    return switch (operationType) {
      case MATCH -> new MatchOperation();
      case MERGE -> new MergeOperation();
      case CREATE -> new CreateOperation();
      case RETURN -> new ReturnOperation();
      case DELETE -> new DeleteOperation();
      case SET -> new SetOperation();
      case ORDER_BY -> new OrderByOperation();
      case EDGE_MATCH -> new EdgeMatchOperation();
      case EDGE_CREATE -> new EdgeCreateOperation();
      case EDGE_MERGE -> new EdgeMergeOperation();
      default ->
          throw new RuntimeException("Operation type " + operationType + " is not supported");
    };
  }

  @Override
  public Object createObject(String id, Object parentObject) throws HopException {
    try {
      OperationType operationType = OperationType.valueOf(id);
      return createOperation(operationType);
    } catch (Exception e) {
      throw new HopException("Error creating operation object ", e);
    }
  }

  @Override
  public String getObjectId(Object object) throws HopException {
    if (object instanceof IOperation iOperation) {
      return iOperation.getOperationType().name();
    } else {
      throw new HopException("Unexpected object class received: " + object.getClass().getName());
    }
  }
}
