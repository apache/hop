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

package org.apache.hop.neo4j.transforms.cypherbuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.IOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.ReturnOperation;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "Neo4jCypherBuilder",
    name = "i18n::CypherBuilderMeta.name",
    description = "i18n::CypherBuilderMeta.description",
    image = "neo4j_cypher.svg",
    categoryDescription = "Neo4j",
    keywords = "i18n::CypherBuilderMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-cypher-builder.html")
public class CypherBuilderMeta extends BaseTransformMeta<CypherBuilder, CypherBuilderData> {
  public static final String ROWS_UNWIND_MAP_ENTRY = "rows";

  /** The name of the Neo4j Connection to use. */
  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
  private String connectionName;

  /** How many cypher statements do we execute at once? */
  @HopMetadataProperty private String batchSize;

  /** If we want to use Cypher UNWIND, set the row alias here. */
  @HopMetadataProperty private String unwindAlias;

  /** The maximum number of retries */
  @HopMetadataProperty private String retries;

  /** The parameters you might want to set. */
  @HopMetadataProperty(groupKey = "parameters", key = "parameter")
  private List<Parameter> parameters;

  /** The operations to perform */
  @HopMetadataProperty(groupKey = "operations", key = "operation")
  private List<IOperation> operations;

  public CypherBuilderMeta() {
    this.parameters = new ArrayList<>();
    this.operations = new ArrayList<>();
  }

  public CypherBuilderMeta(CypherBuilderMeta meta) {
    this();
    this.connectionName = meta.connectionName;
    this.batchSize = meta.batchSize;
    this.unwindAlias = meta.unwindAlias;
    this.retries = meta.retries;
    meta.parameters.forEach(p -> this.parameters.add(p.clone()));
    meta.operations.forEach(o -> this.operations.add(o.clone()));
  }

  @Override
  public void setDefault() {
    batchSize = "1000";
    unwindAlias = "row";
    retries = "10";
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // We can't associate input rows with the list of return records in the case we're using UNWIND
    //
    if (StringUtils.isNotEmpty(variables.resolve(unwindAlias))) {
      inputRowMeta.clear();
    }

    // Look for return values in the operations.
    // These will be returned as fields.
    //
    for (IOperation operation : operations) {
      if (operation instanceof ReturnOperation returnOperation) {
        List<ReturnValue> returnValues = returnOperation.getReturnValues();
        for (ReturnValue returnValue : returnValues) {
          IValueMeta valueMeta = returnValue.createValueMeta();
          valueMeta.setOrigin(name);
          // Store the source data type in the comments
          //
          valueMeta.setComments(returnValue.getNeoType());
          inputRowMeta.addValueMeta(valueMeta);
        }
      }
    }
  }

  public String getCypher(IVariables variables) throws HopException {
    String realUnwindAlias = variables.resolve(unwindAlias);

    StringBuilder cypher = new StringBuilder();
    if (StringUtils.isNotEmpty(realUnwindAlias)) {
      cypher
          .append("UNWIND $")
          .append(ROWS_UNWIND_MAP_ENTRY)
          .append(" AS ")
          .append(realUnwindAlias)
          .append(" ")
          .append(Const.CR);
    }

    for (IOperation operation : operations) {
      cypher.append(operation.getCypherClause(realUnwindAlias, parameters));
      cypher.append(Const.CR);
    }

    return cypher.toString();
  }

  public boolean needsWriteTransaction() {
    for (IOperation operation : operations) {
      if (operation.needsWriteTransaction()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public CypherBuilderMeta clone() {
    return new CypherBuilderMeta(this);
  }

  public IOperation findOperation(String operationName) {
    for (IOperation operation : operations) {
      if (operation.getName() != null && operation.getName().equals(operationName)) {
        return operation;
      }
    }
    return null;
  }

  public String[] getParameterNames() {
    String[] names = new String[parameters.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = parameters.get(i).getName();
    }
    Arrays.sort(names);
    return names;
  }

  public String[] getOperationNames() {
    String[] names = new String[operations.size()];
    for (int i = 0; i < names.length; i++) {
      IOperation operation = operations.get(i);
      names[i] = Const.NVL(operation.getName(), operation.getOperationType().getDescription());
    }
    return names;
  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * Sets connectionName
   *
   * @param connectionName value of connectionName
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets batchSize
   *
   * @return value of batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * Sets batchSize
   *
   * @param batchSize value of batchSize
   */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Gets unwindAlias
   *
   * @return value of unwindAlias
   */
  public String getUnwindAlias() {
    return unwindAlias;
  }

  /**
   * Sets unwindAlias
   *
   * @param unwindAlias value of unwindAlias
   */
  public void setUnwindAlias(String unwindAlias) {
    this.unwindAlias = unwindAlias;
  }

  /**
   * Gets retries
   *
   * @return value of retries
   */
  public String getRetries() {
    return retries;
  }

  /**
   * Sets retries
   *
   * @param retries value of retries
   */
  public void setRetries(String retries) {
    this.retries = retries;
  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public List<Parameter> getParameters() {
    return parameters;
  }

  /**
   * Sets parameters
   *
   * @param parameters value of parameters
   */
  public void setParameters(List<Parameter> parameters) {
    this.parameters = parameters;
  }

  /**
   * Gets operations
   *
   * @return value of operations
   */
  public List<IOperation> getOperations() {
    return operations;
  }

  /**
   * Sets operations
   *
   * @param operations value of operations
   */
  public void setOperations(List<IOperation> operations) {
    this.operations = operations;
  }
}
