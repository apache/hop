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

package org.apache.hop.pipeline.transforms.splunkinput;

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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "SplunkInput",
    name = "Splunk Input",
    description = "Read data from Splunk",
    image = "splunk.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/splunkinput.html")
@InjectionSupported(
    localizationPrefix = "Splunk.Injection.",
    groups = {"PARAMETERS", "RETURNS"})
public class SplunkInputMeta extends BaseTransformMeta
    implements ITransformMeta<SplunkInput, SplunkInputData> {

  public static final String CONNECTION = "connection";
  public static final String QUERY = "query";
  public static final String RETURNS = "returns";
  public static final String RETURN = "return";
  public static final String RETURN_NAME = "return_name";
  public static final String RETURN_SPLUNK_NAME = "return_splunk_name";
  public static final String RETURN_TYPE = "return_type";
  public static final String RETURN_LENGTH = "return_length";
  public static final String RETURN_FORMAT = "return_format";

  @Injection(name = CONNECTION)
  private String connectionName;

  @Injection(name = QUERY)
  private String query;

  @InjectionDeep private List<ReturnValue> returnValues;

  public SplunkInputMeta() {
    super();
    returnValues = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    query = "search * | head 100";
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SplunkInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SplunkInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public SplunkInputData getTransformData() {
    return new SplunkInputData();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    for (ReturnValue returnValue : returnValues) {
      try {
        int type = ValueMetaFactory.getIdForValueMeta(returnValue.getType());
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(returnValue.getName(), type);
        valueMeta.setLength(returnValue.getLength());
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

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append(XmlHandler.addTagValue(CONNECTION, connectionName));
    xml.append(XmlHandler.addTagValue(QUERY, query));

    xml.append(XmlHandler.openTag(RETURNS));
    for (ReturnValue returnValue : returnValues) {
      xml.append(XmlHandler.openTag(RETURN));
      xml.append(XmlHandler.addTagValue(RETURN_NAME, returnValue.getName()));
      xml.append(XmlHandler.addTagValue(RETURN_SPLUNK_NAME, returnValue.getSplunkName()));
      xml.append(XmlHandler.addTagValue(RETURN_TYPE, returnValue.getType()));
      xml.append(XmlHandler.addTagValue(RETURN_LENGTH, returnValue.getLength()));
      xml.append(XmlHandler.addTagValue(RETURN_FORMAT, returnValue.getFormat()));
      xml.append(XmlHandler.closeTag(RETURN));
    }
    xml.append(XmlHandler.closeTag(RETURNS));

    return xml.toString();
  }

  @Override
  public void loadXml(Node stepnode, IHopMetadataProvider provider) throws HopXmlException {
    connectionName = XmlHandler.getTagValue(stepnode, CONNECTION);
    query = XmlHandler.getTagValue(stepnode, QUERY);

    // Parse return values
    //
    Node returnsNode = XmlHandler.getSubNode(stepnode, RETURNS);
    List<Node> returnNodes = XmlHandler.getNodes(returnsNode, RETURN);
    returnValues = new ArrayList<>();
    for (Node returnNode : returnNodes) {
      String name = XmlHandler.getTagValue(returnNode, RETURN_NAME);
      String splunkName = XmlHandler.getTagValue(returnNode, RETURN_SPLUNK_NAME);
      String type = XmlHandler.getTagValue(returnNode, RETURN_TYPE);
      int length = Const.toInt(XmlHandler.getTagValue(returnNode, RETURN_LENGTH), -1);
      String format = XmlHandler.getTagValue(returnNode, RETURN_FORMAT);
      returnValues.add(new ReturnValue(name, splunkName, type, length, format));
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

  /** @param connectionName The connectionName to set */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets query
   *
   * @return value of query
   */
  public String getQuery() {
    return query;
  }

  /** @param query The query to set */
  public void setQuery(String query) {
    this.query = query;
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
}
