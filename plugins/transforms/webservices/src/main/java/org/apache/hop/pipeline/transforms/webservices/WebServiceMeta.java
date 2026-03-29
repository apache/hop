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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "WebServiceLookup",
    image = "webservice.svg",
    name = "i18n::WebServiceLookup.Name",
    description = "i18n::WebServiceLookup.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::WebServiceMeta.keyword",
    documentationUrl = "/pipeline/transforms/webservices.html")
@Getter
@Setter
public class WebServiceMeta extends BaseTransformMeta<WebService, WebServiceData> {
  public static final String XSD_NS_URI = "http://www.w3.org/2001/XMLSchema";

  public static final int DEFAULT_TRANSFORM = 1000;
  public static final String CONST_SPACES = "        ";
  public static final String CONST_WS_NAME = "wsName";
  public static final String CONST_XSD_TYPE = "xsdType";
  public static final String CONST_FIELD = "field";

  /** The input web service fields */
  @HopMetadataProperty(key = "field", groupKey = "fieldsIn")
  private List<WebServiceField> fieldsIn;

  /** The output web service fields */
  @HopMetadataProperty(key = "field", groupKey = "fieldsOut")
  private List<WebServiceField> fieldsOut;

  /** Web service URL */
  @HopMetadataProperty(key = "wsURL")
  private String url;

  /** Name of the web service operation to use */
  @HopMetadataProperty(key = "wsOperation")
  private String operationName;

  /** Name of the operation request name: optional, can be different from the operation name */
  @HopMetadataProperty(key = "wsOperationRequest")
  private String operationRequestName;

  /** The name-variables of the operation */
  @HopMetadataProperty(key = "wsOperationNamespace")
  private String operationNamespace;

  /**
   * The name of the object that encapsulates the input fields in case we're dealing with a table
   */
  @HopMetadataProperty(key = "wsInFieldContainer")
  private String inFieldContainerName;

  /** Name of the input object */
  @HopMetadataProperty(key = "wsInFieldArgument")
  private String inFieldArgumentName;

  /** Name of the object that encapsulates the output fields in case we're dealing with a table */
  @HopMetadataProperty(key = "wsOutFieldContainer")
  private String outFieldContainerName;

  /** Name of the output object */
  @HopMetadataProperty(key = "wsOutFieldArgument")
  private String outFieldArgumentName;

  @HopMetadataProperty(key = "proxyHost")
  private String proxyHost;

  @HopMetadataProperty(key = "proxyPort")
  private String proxyPort;

  @HopMetadataProperty(key = "httpLogin")
  private String httpLogin;

  @HopMetadataProperty(key = "httpPassword", password = true)
  private String httpPassword;

  /** Flag to allow input data to pass to the output */
  @HopMetadataProperty(key = "passingInputData")
  private boolean passingInputData;

  /** The number of rows to send with each call */
  @HopMetadataProperty(key = "callTransform")
  private int callTransform;

  /** Use the 2.5/3.0 parsing logic (available for compatibility reasons) */
  @HopMetadataProperty(key = "compatible")
  private boolean compatible;

  /** The name of the repeating element name. Empty = a single row return */
  @HopMetadataProperty(key = "repeating_element")
  private String repeatingElementName;

  /** Is this transform giving back the complete reply from the service as an XML string? */
  @HopMetadataProperty(key = "reply_as_string")
  private boolean returningReplyAsString;

  public WebServiceMeta() {
    super();
    callTransform = DEFAULT_TRANSFORM;
    fieldsIn = new ArrayList<>();
    fieldsOut = new ArrayList<>();
  }

  public WebServiceMeta(WebServiceMeta m) {
    this();
    this.callTransform = m.callTransform;
    this.compatible = m.compatible;
    this.httpLogin = m.httpLogin;
    this.httpPassword = m.httpPassword;
    this.inFieldArgumentName = m.inFieldArgumentName;
    this.inFieldContainerName = m.inFieldContainerName;
    this.operationName = m.operationName;
    this.operationNamespace = m.operationNamespace;
    this.operationRequestName = m.operationRequestName;
    this.outFieldArgumentName = m.outFieldArgumentName;
    this.outFieldContainerName = m.outFieldContainerName;
    this.passingInputData = m.passingInputData;
    this.proxyHost = m.proxyHost;
    this.proxyPort = m.proxyPort;
    this.repeatingElementName = m.repeatingElementName;
    this.returningReplyAsString = m.returningReplyAsString;
    this.url = m.url;
    m.fieldsIn.forEach(field -> fieldsIn.add(new WebServiceField(field)));
    m.fieldsOut.forEach(field -> fieldsOut.add(new WebServiceField(field)));
  }

  @Override
  public WebServiceMeta clone() {
    return new WebServiceMeta(this);
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

  @Override
  public void setDefault() {
    passingInputData = true; // Pass input data by default.
  }

  @Override
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
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transforms!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else if (getInFieldArgumentName() != null || getInFieldContainerName() != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }
  }

  public WebServiceField getFieldInFromName(String name) {
    WebServiceField param = null;
    for (WebServiceField paramCour : getFieldsIn()) {
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
   * @return The field for the given web service
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
    for (WebServiceField paramCour : getFieldsOut()) {
      if (paramCour.getWsName().equals(wsName)) {
        param = paramCour;
        break;
      }
    }
    return param;
  }

  public boolean hasFieldsIn() {
    return !Utils.isEmpty(fieldsIn);
  }

  public void addFieldIn(WebServiceField field) {
    fieldsIn.add(field);
  }

  public void addFieldOut(WebServiceField field) {
    fieldsOut.add(field);
  }
}
