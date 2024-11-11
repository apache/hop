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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "Neo4jCypherOutput",
    name = "i18n::Cypher.Transform.Name",
    description = "i18n::Cypher.Transform.Description",
    image = "neo4j_cypher.svg",
    categoryDescription = "Neo4j",
    keywords = "i18n::CypherMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-cypher.html")
public class CypherMeta extends BaseTransformMeta<Cypher, CypherData> {
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "connection",
      injectionKeyDescription = "Cypher.Injection.connection",
      hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
  private String connectionName;

  @HopMetadataProperty(
      key = "cypher",
      injectionKey = "cypher",
      injectionKeyDescription = "Cypher.Injection.cypher")
  private String cypher;

  @HopMetadataProperty(
      key = "batch_size",
      injectionKey = "batch_size",
      injectionKeyDescription = "Cypher.Injection.batch_size")
  private String batchSize;

  @HopMetadataProperty(
      key = "read_only",
      injectionKey = "read_only",
      injectionKeyDescription = "Cypher.Injection.read_only")
  private boolean readOnly;

  @HopMetadataProperty(
      key = "retry",
      injectionKey = "retry",
      injectionKeyDescription = "Cypher.Injection.retry")
  private boolean retryingOnDisconnect;

  @HopMetadataProperty(
      key = "nr_retries_on_error",
      injectionKey = "nr_retries_on_error",
      injectionKeyDescription = "Cypher.Injection.nr_retries_on_error")
  private String nrRetriesOnError;

  @HopMetadataProperty(
      key = "cypher_from_field",
      injectionKey = "cypher_from_field",
      injectionKeyDescription = "Cypher.Injection.cypher_from_field")
  private boolean cypherFromField;

  @HopMetadataProperty(
      key = "cypher_field",
      injectionKey = "cypher_field",
      injectionKeyDescription = "Cypher.Injection.cypher_field")
  private String cypherField;

  @HopMetadataProperty(
      key = "unwind",
      injectionKey = "unwind",
      injectionKeyDescription = "Cypher.Injection.unwind")
  private boolean usingUnwind;

  @HopMetadataProperty(
      key = "unwind_map",
      injectionKey = "unwind_map",
      injectionKeyDescription = "Cypher.Injection.unwind_map")
  private String unwindMapName;

  @HopMetadataProperty(
      key = "returning_graph",
      injectionKey = "returning_graph",
      injectionKeyDescription = "Cypher.Injection.returning_graph")
  private boolean returningGraph;

  @HopMetadataProperty(
      key = "return_graph_field",
      injectionKey = "return_graph_field",
      injectionKeyDescription = "Cypher.Injection.return_graph_field")
  private String returnGraphField;

  @HopMetadataProperty(
      groupKey = "mappings",
      key = "mapping",
      injectionGroupKey = "PARAMETERS",
      injectionGroupDescription = "Cypher.Injection.PARAMETERS")
  private List<ParameterMapping> parameterMappings;

  @HopMetadataProperty(
      groupKey = "returns",
      key = "return",
      injectionGroupKey = "RETURNS",
      injectionKeyDescription = "Cypher.Injection.RETURNS")
  private List<ReturnValue> returnValues;

  public CypherMeta() {
    super();
    parameterMappings = new ArrayList<>();
    returnValues = new ArrayList<>();
  }

  public CypherMeta(CypherMeta m) {
    this();
    this.connectionName = m.connectionName;
    this.cypher = m.cypher;
    this.batchSize = m.batchSize;
    this.readOnly = m.readOnly;
    this.retryingOnDisconnect = m.retryingOnDisconnect;
    this.nrRetriesOnError = m.nrRetriesOnError;
    this.cypherFromField = m.cypherFromField;
    this.cypherField = m.cypherField;
    this.usingUnwind = m.usingUnwind;
    this.unwindMapName = m.unwindMapName;
    this.returningGraph = m.returningGraph;
    this.returnGraphField = m.returnGraphField;
    m.parameterMappings.forEach(p -> this.parameterMappings.add(new ParameterMapping(p)));
    m.returnValues.forEach(v -> this.returnValues.add(new ReturnValue(v)));
  }

  @Override
  public CypherMeta clone() {
    return new CypherMeta(this);
  }

  @Override
  public void setDefault() {
    retryingOnDisconnect = true;
  }

  @Override
  public String getDialogClassName() {
    return CypherDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (usingUnwind) {
      // Unwind only outputs results, not input
      //
      rowMeta.clear();
    }

    if (returningGraph) {
      // We send out a single Graph value per input row
      //
      IValueMeta valueMetaGraph = new ValueMetaGraph(Const.NVL(returnGraphField, "graph"));
      valueMetaGraph.setOrigin(name);
      rowMeta.addValueMeta(valueMetaGraph);
    } else {
      // Check return values in the metadata...
      //
      for (ReturnValue returnValue : returnValues) {
        try {
          int type = ValueMetaFactory.getIdForValueMeta(returnValue.getType());
          IValueMeta valueMeta = ValueMetaFactory.createValueMeta(returnValue.getName(), type);
          valueMeta.setOrigin(name);
          rowMeta.addValueMeta(valueMeta);
        } catch (HopPluginException e) {
          throw new HopTransformException(
              "Unknown data type '"
                  + returnValue.getType()
                  + "' for value named '"
                  + returnValue.getName()
                  + "'");
        }
      }
    }
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
   * @param connectionName The connectionName to set
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets cypher
   *
   * @return value of cypher
   */
  public String getCypher() {
    return cypher;
  }

  /**
   * @param cypher The cypher to set
   */
  public void setCypher(String cypher) {
    this.cypher = cypher;
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
   * @param batchSize The batchSize to set
   */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Gets cypherFromField
   *
   * @return value of cypherFromField
   */
  public boolean isCypherFromField() {
    return cypherFromField;
  }

  /**
   * @param cypherFromField The cypherFromField to set
   */
  public void setCypherFromField(boolean cypherFromField) {
    this.cypherFromField = cypherFromField;
  }

  /**
   * Gets readOnly
   *
   * @return value of readOnly
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * @param readOnly The readOnly to set
   */
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * Gets retrying
   *
   * @return value of retrying
   */
  public boolean isRetryingOnDisconnect() {
    return retryingOnDisconnect;
  }

  /**
   * @param retryingOnDisconnect The retrying to set
   */
  public void setRetryingOnDisconnect(boolean retryingOnDisconnect) {
    this.retryingOnDisconnect = retryingOnDisconnect;
  }

  /**
   * Gets cypherField
   *
   * @return value of cypherField
   */
  public String getCypherField() {
    return cypherField;
  }

  /**
   * @param cypherField The cypherField to set
   */
  public void setCypherField(String cypherField) {
    this.cypherField = cypherField;
  }

  /**
   * Gets usingUnwind
   *
   * @return value of usingUnwind
   */
  public boolean isUsingUnwind() {
    return usingUnwind;
  }

  /**
   * @param usingUnwind The usingUnwind to set
   */
  public void setUsingUnwind(boolean usingUnwind) {
    this.usingUnwind = usingUnwind;
  }

  /**
   * Gets unwindMapName
   *
   * @return value of unwindMapName
   */
  public String getUnwindMapName() {
    return unwindMapName;
  }

  /**
   * Gets returningGraph
   *
   * @return value of returningGraph
   */
  public boolean isReturningGraph() {
    return returningGraph;
  }

  /**
   * @param returningGraph The returningGraph to set
   */
  public void setReturningGraph(boolean returningGraph) {
    this.returningGraph = returningGraph;
  }

  /**
   * @param unwindMapName The unwindMapName to set
   */
  public void setUnwindMapName(String unwindMapName) {
    this.unwindMapName = unwindMapName;
  }

  /**
   * Gets parameterMappings
   *
   * @return value of parameterMappings
   */
  public List<ParameterMapping> getParameterMappings() {
    return parameterMappings;
  }

  /**
   * @param parameterMappings The parameterMappings to set
   */
  public void setParameterMappings(List<ParameterMapping> parameterMappings) {
    this.parameterMappings = parameterMappings;
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
   * @param returnValues The returnValues to set
   */
  public void setReturnValues(List<ReturnValue> returnValues) {
    this.returnValues = returnValues;
  }

  /**
   * Gets returnGraphField
   *
   * @return value of returnGraphField
   */
  public String getReturnGraphField() {
    return returnGraphField;
  }

  /**
   * @param returnGraphField The returnGraphField to set
   */
  public void setReturnGraphField(String returnGraphField) {
    this.returnGraphField = returnGraphField;
  }

  /**
   * Gets nrRetriesOnError
   *
   * @return value of nrRetriesOnError
   */
  public String getNrRetriesOnError() {
    return nrRetriesOnError;
  }

  /**
   * @param nrRetriesOnError The nrRetriesOnError to set
   */
  public void setNrRetriesOnError(String nrRetriesOnError) {
    this.nrRetriesOnError = nrRetriesOnError;
  }
}
