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
 */

package org.apache.hop.pipeline.transforms.webservices;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Transform(
    id = "WebServiceLookup",
    image = "webservice.svg",
    name = "i18n::BaseTransform.TypeLongDesc.WebServiceLookup",
    description = "i18n::BaseTransform.TypeTooltipDesc.WebServiceLookup",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/webservices.html")
public class WebServiceMeta extends BaseTransformMeta
    implements ITransformMeta<WebService, WebServiceData> {
  public static final String XSD_NS_URI = "http://www.w3.org/2001/XMLSchema";

  public static final int DEFAULT_TRANSFORM = 1000;

  /** The input web service fields */
  private List<WebServiceField> fieldsIn;

  /** The output web service fields */
  private List<WebServiceField> fieldsOut;

  /** Web service URL */
  private String url;

  /** Name of the web service operation to use */
  private String operationName;

  /** Name of the operation request name: optional, can be different from the operation name */
  private String operationRequestName;

  /** The name-variables of the operation */
  private String operationNamespace;

  /**
   * The name of the object that encapsulates the input fields in case we're dealing with a table
   */
  private String inFieldContainerName;

  /** Name of the input object */
  private String inFieldArgumentName;

  /** Name of the object that encapsulates the output fields in case we're dealing with a table */
  private String outFieldContainerName;

  /** Name of the output object */
  private String outFieldArgumentName;

  private String proxyHost;

  private String proxyPort;

  private String httpLogin;

  private String httpPassword;

  /** Flag to allow input data to pass to the output */
  private boolean passingInputData;

  /** The number of rows to send with each call */
  private int callTransform = DEFAULT_TRANSFORM;

  /** Use the 2.5/3.0 parsing logic (available for compatibility reasons) */
  private boolean compatible;

  /** The name of the repeating element name. Empty = a single row return */
  private String repeatingElementName;

  /** Is this transform giving back the complete reply from the service as an XML string? */
  private boolean returningReplyAsString;

  public WebServiceMeta() {
    super();
    fieldsIn = new ArrayList<>();
    fieldsOut = new ArrayList<>();
  }

  public WebServiceMeta(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    this();
    loadXml(transformNode, metadataProvider);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Input rows and output rows are different in the webservice transform
    //
    if (!isPassingInputData()) {
      r.clear();
    }

    // Add the output fields...
    //
    for (WebServiceField field : getFieldsOut()) {
      int valueType = field.getType();

      // If the type is unrecognized we give back the XML as a String...
      //
      if (field.getType() == IValueMeta.TYPE_NONE) {
        valueType = IValueMeta.TYPE_STRING;
      }

      try {
        IValueMeta vValue = ValueMetaFactory.createValueMeta(field.getName(), valueType);
        vValue.setOrigin(name);
        r.addValueMeta(vValue);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
  }

  public WebServiceMeta clone() {
    WebServiceMeta retval = (WebServiceMeta) super.clone();
    retval.fieldsIn = new ArrayList<>();
    for (WebServiceField field : fieldsIn) {
      retval.fieldsIn.add(field.clone());
    }
    retval.fieldsOut = new ArrayList<>();
    for (WebServiceField field : fieldsOut) {
      retval.fieldsOut.add(field.clone());
    }
    return retval;
  }

  public void setDefault() {
    passingInputData = true; // Pass input data by default.
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transforms!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else if (getInFieldArgumentName() != null || getInFieldContainerName() != null) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    // Store the WebService URL
    //
    retval.append("    " + XmlHandler.addTagValue("wsURL", getUrl()));

    // Store the operation
    //
    retval.append("    " + XmlHandler.addTagValue("wsOperation", getOperationName()));
    retval.append("    " + XmlHandler.addTagValue("wsOperationRequest", getOperationRequestName()));
    retval.append("    " + XmlHandler.addTagValue("wsOperationNamespace", getOperationNamespace()));
    retval.append("    " + XmlHandler.addTagValue("wsInFieldContainer", getInFieldContainerName()));
    retval.append("    " + XmlHandler.addTagValue("wsInFieldArgument", getInFieldArgumentName()));
    retval.append(
        "    " + XmlHandler.addTagValue("wsOutFieldContainer", getOutFieldContainerName()));
    retval.append("    " + XmlHandler.addTagValue("wsOutFieldArgument", getOutFieldArgumentName()));
    retval.append("    " + XmlHandler.addTagValue("proxyHost", getProxyHost()));
    retval.append("    " + XmlHandler.addTagValue("proxyPort", getProxyPort()));
    retval.append("    " + XmlHandler.addTagValue("httpLogin", getHttpLogin()));
    retval.append("    " + XmlHandler.addTagValue("httpPassword", getHttpPassword()));
    retval.append("    " + XmlHandler.addTagValue("callTransform", getCallTransform()));
    retval.append("    " + XmlHandler.addTagValue("passingInputData", isPassingInputData()));
    retval.append("    " + XmlHandler.addTagValue("compatible", isCompatible()));
    retval.append("    " + XmlHandler.addTagValue("repeating_element", getRepeatingElementName()));
    retval.append("    " + XmlHandler.addTagValue("reply_as_string", isReturningReplyAsString()));

    // Store the field parameters
    //

    // Store the link between the input fields and the WebService input
    //
    retval.append("    <fieldsIn>" + Const.CR);
    for (int i = 0; i < getFieldsIn().size(); i++) {
      WebServiceField vField = getFieldsIn().get(i);
      retval.append("    <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", vField.getName()));
      retval.append("        " + XmlHandler.addTagValue("wsName", vField.getWsName()));
      retval.append("        " + XmlHandler.addTagValue("xsdType", vField.getXsdType()));
      retval.append("    </field>" + Const.CR);
    }
    retval.append("      </fieldsIn>" + Const.CR);

    // Store the link between the input fields and the WebService output
    //
    retval.append("    <fieldsOut>" + Const.CR);
    for (int i = 0; i < getFieldsOut().size(); i++) {
      WebServiceField vField = getFieldsOut().get(i);
      retval.append("    <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", vField.getName()));
      retval.append("        " + XmlHandler.addTagValue("wsName", vField.getWsName()));
      retval.append("        " + XmlHandler.addTagValue("xsdType", vField.getXsdType()));
      retval.append("    </field>" + Const.CR);
    }
    retval.append("      </fieldsOut>" + Const.CR);

    return retval.toString();
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    // Load the URL
    //
    setUrl(XmlHandler.getTagValue(transformNode, "wsURL"));

    // Load the operation
    //
    setOperationName(XmlHandler.getTagValue(transformNode, "wsOperation"));
    setOperationRequestName(XmlHandler.getTagValue(transformNode, "wsOperationRequest"));
    setOperationNamespace(XmlHandler.getTagValue(transformNode, "wsOperationNamespace"));
    setInFieldContainerName(XmlHandler.getTagValue(transformNode, "wsInFieldContainer"));
    setInFieldArgumentName(XmlHandler.getTagValue(transformNode, "wsInFieldArgument"));
    setOutFieldContainerName(XmlHandler.getTagValue(transformNode, "wsOutFieldContainer"));
    setOutFieldArgumentName(XmlHandler.getTagValue(transformNode, "wsOutFieldArgument"));
    setProxyHost(XmlHandler.getTagValue(transformNode, "proxyHost"));
    setProxyPort(XmlHandler.getTagValue(transformNode, "proxyPort"));
    setHttpLogin(XmlHandler.getTagValue(transformNode, "httpLogin"));
    setHttpPassword(XmlHandler.getTagValue(transformNode, "httpPassword"));
    setCallTransform(
        Const.toInt(XmlHandler.getTagValue(transformNode, "callTransform"), DEFAULT_TRANSFORM));
    setPassingInputData(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "passingInputData")));
    String compat = XmlHandler.getTagValue(transformNode, "compatible");
    setCompatible(Utils.isEmpty(compat) || "Y".equalsIgnoreCase(compat));
    setRepeatingElementName(XmlHandler.getTagValue(transformNode, "repeating_element"));
    setReturningReplyAsString(
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "reply_as_string")));

    // Load the input fields mapping
    //
    getFieldsIn().clear();
    Node fields = XmlHandler.getSubNode(transformNode, "fieldsIn");
    int nrFields = XmlHandler.countNodes(fields, "field");

    for (int i = 0; i < nrFields; ++i) {
      Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

      WebServiceField field = new WebServiceField();
      field.setName(XmlHandler.getTagValue(fnode, "name"));
      field.setWsName(XmlHandler.getTagValue(fnode, "wsName"));
      field.setXsdType(XmlHandler.getTagValue(fnode, "xsdType"));
      getFieldsIn().add(field);
    }

    // Load the output fields mapping
    //
    getFieldsOut().clear();

    fields = XmlHandler.getSubNode(transformNode, "fieldsOut");
    nrFields = XmlHandler.countNodes(fields, "field");

    for (int i = 0; i < nrFields; ++i) {
      Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

      WebServiceField field = new WebServiceField();
      field.setName(XmlHandler.getTagValue(fnode, "name"));
      field.setWsName(XmlHandler.getTagValue(fnode, "wsName"));
      field.setXsdType(XmlHandler.getTagValue(fnode, "xsdType"));
      getFieldsOut().add(field);
    }
  }

  public String getOperationName() {
    return operationName;
  }

  public void setOperationName(String operationName) {
    this.operationName = operationName;
  }

  @Override
  public WebService createTransform(
      TransformMeta transformMeta,
      WebServiceData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline disp) {
    return new WebService(transformMeta, this, data, cnr, pipelineMeta, disp);
  }

  public WebServiceData getTransformData() {
    return new WebServiceData();
  }

  public WebServiceField getFieldInFromName(String name) {
    WebServiceField param = null;
    for (Iterator<WebServiceField> iter = getFieldsIn().iterator(); iter.hasNext(); ) {
      WebServiceField paramCour = iter.next();
      if (name.equals(paramCour.getName())) {
        param = paramCour;
        break;
      }
    }
    return param;
  }

  /**
   * Returns the WebServicesField for the given wsName.
   *
   * @param wsName The name of the WebServiceField to return
   * @param ignoreWsNsPrefix If true the lookup of the cache of WebServiceFields will not include
   *     the target namespace prefix.
   * @return
   */
  public WebServiceField getFieldOutFromWsName(String wsName, boolean ignoreWsNsPrefix) {
    WebServiceField param = null;

    if (Utils.isEmpty(wsName)) {
      return param;
    }

    // if we are ignoring the name variables prefix
    if (ignoreWsNsPrefix) {

      // we split the wsName and set it to the last element of what was parsed
      String[] wsNameParsed = wsName.split(":");
      wsName = wsNameParsed[wsNameParsed.length - 1];
    }

    // we now look for the wsname
    for (Iterator<WebServiceField> iter = getFieldsOut().iterator(); iter.hasNext(); ) {
      WebServiceField paramCour = iter.next();
      if (paramCour.getWsName().equals(wsName)) {
        param = paramCour;
        break;
      }
    }
    return param;
  }

  public List<WebServiceField> getFieldsIn() {
    return fieldsIn;
  }

  public void setFieldsIn(List<WebServiceField> fieldsIn) {
    this.fieldsIn = fieldsIn;
  }

  public boolean hasFieldsIn() {
    return fieldsIn != null && !fieldsIn.isEmpty();
  }

  public void addFieldIn(WebServiceField field) {
    fieldsIn.add(field);
  }

  public List<WebServiceField> getFieldsOut() {
    return fieldsOut;
  }

  public void setFieldsOut(List<WebServiceField> fieldsOut) {
    this.fieldsOut = fieldsOut;
  }

  public void addFieldOut(WebServiceField field) {
    fieldsOut.add(field);
  }

  public String getInFieldArgumentName() {
    return inFieldArgumentName;
  }

  public void setInFieldArgumentName(String inFieldArgumentName) {
    this.inFieldArgumentName = inFieldArgumentName;
  }

  public String getOutFieldArgumentName() {
    return outFieldArgumentName;
  }

  public void setOutFieldArgumentName(String outFieldArgumentName) {
    this.outFieldArgumentName = outFieldArgumentName;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public int getCallTransform() {
    return callTransform;
  }

  public void setCallTransform(int callTransform) {
    this.callTransform = callTransform;
  }

  public String getOperationNamespace() {
    return operationNamespace;
  }

  public void setOperationNamespace(String operationNamespace) {
    this.operationNamespace = operationNamespace;
  }

  public String getHttpLogin() {
    return httpLogin;
  }

  public void setHttpLogin(String httpLogin) {
    this.httpLogin = httpLogin;
  }

  public String getHttpPassword() {
    return httpPassword;
  }

  public void setHttpPassword(String httpPassword) {
    this.httpPassword = httpPassword;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  public String getInFieldContainerName() {
    return inFieldContainerName;
  }

  public void setInFieldContainerName(String inFieldContainerName) {
    this.inFieldContainerName = inFieldContainerName;
  }

  public String getOutFieldContainerName() {
    return outFieldContainerName;
  }

  public void setOutFieldContainerName(String outFieldContainerName) {
    this.outFieldContainerName = outFieldContainerName;
  }

  /** @return the passingInputData */
  public boolean isPassingInputData() {
    return passingInputData;
  }

  /** @param passingInputData the passingInputData to set */
  public void setPassingInputData(boolean passingInputData) {
    this.passingInputData = passingInputData;
  }

  /** @return the compatible */
  public boolean isCompatible() {
    return compatible;
  }

  /** @param compatible the compatible to set */
  public void setCompatible(boolean compatible) {
    this.compatible = compatible;
  }

  /** @return the repeatingElementName */
  public String getRepeatingElementName() {
    return repeatingElementName;
  }

  /** @param repeatingElementName the repeatingElementName to set */
  public void setRepeatingElementName(String repeatingElementName) {
    this.repeatingElementName = repeatingElementName;
  }

  /** @return true if the reply from the service is simply passed on as a String, mostly in XML */
  public boolean isReturningReplyAsString() {
    return returningReplyAsString;
  }

  /**
   * @param returningReplyAsString true if the reply from the service is simply passed on as a
   *     String, mostly in XML
   */
  public void setReturningReplyAsString(boolean returningReplyAsString) {
    this.returningReplyAsString = returningReplyAsString;
  }

  /** @return the operationRequestName */
  public String getOperationRequestName() {
    return operationRequestName;
  }

  /** @param operationRequestName the operationRequestName to set */
  public void setOperationRequestName(String operationRequestName) {
    this.operationRequestName = operationRequestName;
  }
}
