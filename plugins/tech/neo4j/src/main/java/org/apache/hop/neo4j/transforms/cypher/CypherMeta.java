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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "Neo4jCypherOutput",
    name = "Neo4j Cypher",
    description =
        "Reads from or writes to Neo4j using Cypher with parameter data from input fields",
    image = "neo4j_cypher.svg",
    categoryDescription = "Neo4j",
    keywords = "i18n::CypherMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-cypher.html")
@InjectionSupported(
    localizationPrefix = "Cypher.Injection.",
    groups = {"PARAMETERS", "RETURNS"})
public class CypherMeta extends BaseTransformMeta<Cypher, CypherData> {

  public static final String CONNECTION = "connection";
  public static final String CYPHER = "cypher";
  public static final String BATCH_SIZE = "batch_size";
  public static final String READ_ONLY = "read_only";
  public static final String RETRY = "retry";
  public static final String NR_RETRIES_ON_ERROR = "nr_retries_on_error";
  public static final String CYPHER_FROM_FIELD = "cypher_from_field";
  public static final String CYPHER_FIELD = "cypher_field";
  public static final String UNWIND = "unwind";
  public static final String UNWIND_MAP = "unwind_map";
  public static final String RETURNING_GRAPH = "returning_graph";
  public static final String RETURN_GRAPH_FIELD = "return_graph_field";
  public static final String MAPPINGS = "mappings";
  public static final String MAPPING = "mapping";
  public static final String PARAMETERS = "parameters";
  public static final String PARAMETER = "parameter";
  public static final String FIELD = "field";
  public static final String TYPE = "type";
  public static final String SOURCE_TYPE = "source_type";
  public static final String RETURNS = "returns";
  public static final String RETURN = "return";
  public static final String NAME = "name";
  public static final String PARAMETER_NAME = "parameter_name";
  public static final String PARAMETER_FIELD = "parameter_field";
  public static final String PARAMETER_TYPE = "parameter_type";

  public static final String RETURN_NAME = "return_name";
  public static final String RETURN_TYPE = "return_type";
  public static final String RETURN_SOURCE_TYPE = "return_source_type";

  @Injection(name = CONNECTION)
  private String connectionName;

  @Injection(name = CYPHER)
  private String cypher;

  @Injection(name = BATCH_SIZE)
  private String batchSize;

  @Injection(name = READ_ONLY)
  private boolean readOnly;

  @Injection(name = RETRY)
  private boolean retryingOnDisconnect;

  @Injection(name = NR_RETRIES_ON_ERROR)
  private String nrRetriesOnError;

  @Injection(name = CYPHER_FROM_FIELD)
  private boolean cypherFromField;

  @Injection(name = CYPHER_FIELD)
  private String cypherField;

  @Injection(name = UNWIND)
  private boolean usingUnwind;

  @Injection(name = UNWIND_MAP)
  private String unwindMapName;

  @Injection(name = RETURNING_GRAPH)
  private boolean returningGraph;

  @Injection(name = RETURN_GRAPH_FIELD)
  private String returnGraphField;

  @InjectionDeep private List<ParameterMapping> parameterMappings;

  @InjectionDeep private List<ReturnValue> returnValues;

  public CypherMeta() {
    super();
    parameterMappings = new ArrayList<>();
    returnValues = new ArrayList<>();
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

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append(XmlHandler.addTagValue(CONNECTION, connectionName));
    xml.append(XmlHandler.addTagValue(CYPHER, cypher));
    xml.append(XmlHandler.addTagValue(BATCH_SIZE, batchSize));
    xml.append(XmlHandler.addTagValue(READ_ONLY, readOnly));
    xml.append(XmlHandler.addTagValue(NR_RETRIES_ON_ERROR, nrRetriesOnError));
    xml.append(XmlHandler.addTagValue(RETRY, retryingOnDisconnect));
    xml.append(XmlHandler.addTagValue(CYPHER_FROM_FIELD, cypherFromField));
    xml.append(XmlHandler.addTagValue(CYPHER_FIELD, cypherField));
    xml.append(XmlHandler.addTagValue(UNWIND, usingUnwind));
    xml.append(XmlHandler.addTagValue(UNWIND_MAP, unwindMapName));
    xml.append(XmlHandler.addTagValue(RETURNING_GRAPH, returningGraph));
    xml.append(XmlHandler.addTagValue(RETURN_GRAPH_FIELD, returnGraphField));

    xml.append(XmlHandler.openTag(MAPPINGS));
    for (ParameterMapping parameterMapping : parameterMappings) {
      xml.append(XmlHandler.openTag(MAPPING));
      xml.append(XmlHandler.addTagValue(PARAMETER, parameterMapping.getParameter()));
      xml.append(XmlHandler.addTagValue(FIELD, parameterMapping.getField()));
      xml.append(XmlHandler.addTagValue(TYPE, parameterMapping.getNeoType()));
      xml.append(XmlHandler.closeTag(MAPPING));
    }
    xml.append(XmlHandler.closeTag(MAPPINGS));

    xml.append(XmlHandler.openTag(RETURNS));
    for (ReturnValue returnValue : returnValues) {
      xml.append(XmlHandler.openTag(RETURN));
      xml.append(XmlHandler.addTagValue(NAME, returnValue.getName()));
      xml.append(XmlHandler.addTagValue(TYPE, returnValue.getType()));
      xml.append(XmlHandler.addTagValue(SOURCE_TYPE, returnValue.getSourceType()));
      xml.append(XmlHandler.closeTag(RETURN));
    }
    xml.append(XmlHandler.closeTag(RETURNS));

    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    connectionName = XmlHandler.getTagValue(transformNode, CONNECTION);
    cypher = XmlHandler.getTagValue(transformNode, CYPHER);
    batchSize = XmlHandler.getTagValue(transformNode, BATCH_SIZE);
    readOnly = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, READ_ONLY));
    String retryString = XmlHandler.getTagValue(transformNode, RETRY);
    retryingOnDisconnect = StringUtils.isEmpty(retryString) || "Y".equalsIgnoreCase(retryString);
    nrRetriesOnError = XmlHandler.getTagValue(transformNode, NR_RETRIES_ON_ERROR);
    cypherFromField =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, CYPHER_FROM_FIELD));
    cypherField = XmlHandler.getTagValue(transformNode, CYPHER_FIELD);
    usingUnwind = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, UNWIND));
    unwindMapName = XmlHandler.getTagValue(transformNode, UNWIND_MAP);
    returningGraph = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, RETURNING_GRAPH));
    returnGraphField = XmlHandler.getTagValue(transformNode, RETURN_GRAPH_FIELD);

    // Parse parameter mappings
    //
    Node mappingsNode = XmlHandler.getSubNode(transformNode, MAPPINGS);
    List<Node> mappingNodes = XmlHandler.getNodes(mappingsNode, MAPPING);
    parameterMappings = new ArrayList<>();
    for (Node mappingNode : mappingNodes) {
      String parameter = XmlHandler.getTagValue(mappingNode, PARAMETER);
      String field = XmlHandler.getTagValue(mappingNode, FIELD);
      String neoType = XmlHandler.getTagValue(mappingNode, TYPE);
      if (StringUtils.isEmpty(neoType)) {
        neoType = GraphPropertyType.String.name();
      }
      parameterMappings.add(new ParameterMapping(parameter, field, neoType));
    }

    // Parse return values
    //
    Node returnsNode = XmlHandler.getSubNode(transformNode, RETURNS);
    List<Node> returnNodes = XmlHandler.getNodes(returnsNode, RETURN);
    returnValues = new ArrayList<>();
    for (Node returnNode : returnNodes) {
      String name = XmlHandler.getTagValue(returnNode, NAME);
      String type = XmlHandler.getTagValue(returnNode, TYPE);
      String sourceType = XmlHandler.getTagValue(returnNode, SOURCE_TYPE);
      returnValues.add(new ReturnValue(name, type, sourceType));
    }

    super.loadXml(transformNode, metadataProvider);
  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /** @param connectionName The connectionName to set */
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

  /** @param cypher The cypher to set */
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

  /** @param batchSize The batchSize to set */
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

  /** @param cypherFromField The cypherFromField to set */
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

  /** @param readOnly The readOnly to set */
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

  /** @param retryingOnDisconnect The retrying to set */
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

  /** @param cypherField The cypherField to set */
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

  /** @param usingUnwind The usingUnwind to set */
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

  /** @param returningGraph The returningGraph to set */
  public void setReturningGraph(boolean returningGraph) {
    this.returningGraph = returningGraph;
  }

  /** @param unwindMapName The unwindMapName to set */
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

  /** @param parameterMappings The parameterMappings to set */
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

  /** @param returnValues The returnValues to set */
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

  /** @param returnGraphField The returnGraphField to set */
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

  /** @param nrRetriesOnError The nrRetriesOnError to set */
  public void setNrRetriesOnError(String nrRetriesOnError) {
    this.nrRetriesOnError = nrRetriesOnError;
  }
}
