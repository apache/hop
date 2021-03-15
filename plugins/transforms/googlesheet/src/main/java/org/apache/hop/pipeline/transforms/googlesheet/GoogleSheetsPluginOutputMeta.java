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
package org.apache.hop.pipeline.transforms.googlesheet;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "GoogleSheetsPluginOutput",
    image = "GoogleSheetsPluginOutput.svg",
    name = "i18n::GoogleSheetsPluginOutput.transform.Name",
    description = "i18n::GoogleSheetsPluginOutput.transform.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
  documentationUrl =
    "https://hop.apache.org/manual/latest/plugins/transforms/google-sheets-output.html")
@InjectionSupported(
    localizationPrefix = "GoogleSheetsPluginOutput.injection.",
    groups = {"SHEET", "INPUT_FIELDS"})
public class GoogleSheetsPluginOutputMeta extends BaseTransformMeta
    implements ITransformMeta<
        GoogleSheetsPluginOutput, GoogleSheetsPluginOutputData> { // <Constant, ConstantData>

  private static Class<?> PKG =
      GoogleSheetsPluginOutputMeta
          .class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public GoogleSheetsPluginOutputMeta() {
    super();
  }

  @Injection(name = "jsonCrendentialPath", group = "SHEET")
  private String jsonCredentialPath;

  @Injection(name = "spreadsheetKey", group = "SHEET")
  private String spreadsheetKey;

  @Injection(name = "worksheetId", group = "SHEET")
  private String worksheetId;

  @Injection(name = "Email", group = "SHEET")
  private String shareEmail;

  @Injection(name = "Domain", group = "SHEET")
  private String shareDomain;

  @Injection(name = "Create", group = "SHEET")
  private Boolean create;

  @Injection(name = "Append", group = "SHEET")
  private Boolean append;

  @Override
  public void setDefault() {
    this.jsonCredentialPath = "" + "/client_secret.json";
    this.spreadsheetKey = "";
    this.worksheetId = "";
    this.shareDomain = "";
    this.shareEmail = "";
    this.create = false;
    this.append = false;
  }

  /* public String getDialogClassName() {
      return "org.pentaho.di.ui.trans.transforms.pentahogooglesheets.PentahoGoogleSheetsPluginOutputDialog";
  }*/

  public String getJsonCredentialPath() {
    return this.jsonCredentialPath == null ? "" : this.jsonCredentialPath;
  }

  public void setJsonCredentialPath(String key) {
    this.jsonCredentialPath = key;
  }

  public String getSpreadsheetKey() {
    return this.spreadsheetKey == null ? "" : this.spreadsheetKey;
  }

  public void setSpreadsheetKey(String key) {
    this.spreadsheetKey = key;
  }

  public String getShareEmail() {
    return this.shareEmail == null ? "" : this.shareEmail;
  }

  public void setShareEmail(String shareEmail) {
    this.shareEmail = shareEmail;
  }

  public String getShareDomain() {
    return this.shareDomain == null ? "" : this.shareDomain;
  }

  public void setShareDomain(String shareDomain) {
    this.shareDomain = shareDomain;
  }

  public Boolean getCreate() {
    return this.create == null ? false : this.create;
  }

  public void setCreate(Boolean create) {
    this.create = create;
  }

  public void setAppend(Boolean append) {
    this.append = append;
  }

  public Boolean getAppend() {
    return this.append == null ? false : this.append;
  }

  public String getWorksheetId() {
    return this.worksheetId == null ? "" : this.worksheetId;
  }

  public void setWorksheetId(String id) {
    this.worksheetId = id;
  }

  @Override
  public Object clone() {
    GoogleSheetsPluginOutputMeta retval = (GoogleSheetsPluginOutputMeta) super.clone();
    retval.setJsonCredentialPath(this.jsonCredentialPath);
    retval.setSpreadsheetKey(this.spreadsheetKey);
    retval.setWorksheetId(this.worksheetId);
    retval.setCreate(this.create);
    retval.setAppend(this.append);
    retval.setShareEmail(this.shareEmail);
    retval.setShareDomain(this.shareDomain);
    return retval;
  }

  @Override
  public String getXml() throws HopException {
    StringBuilder xml = new StringBuilder();
    try {
      xml.append(XmlHandler.addTagValue("jsonCredentialPath", this.jsonCredentialPath));
      xml.append(XmlHandler.addTagValue("worksheetId", this.worksheetId));
      xml.append(XmlHandler.addTagValue("spreadsheetKey", this.spreadsheetKey));
      xml.append(XmlHandler.addTagValue("CREATE", Boolean.toString(this.create)));
      xml.append(XmlHandler.addTagValue("APPEND", Boolean.toString(this.append)));
      xml.append(XmlHandler.addTagValue("SHAREEMAIL", this.shareEmail));
      xml.append(XmlHandler.addTagValue("SHAREDOMAIN", this.shareDomain));
    } catch (Exception e) {
      throw new HopValueException("Unable to write transform to XML", e);
    }
    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      this.jsonCredentialPath = XmlHandler.getTagValue(transformNode, "jsonCredentialPath");
      this.worksheetId = XmlHandler.getTagValue(transformNode, "worksheetId");
      this.spreadsheetKey = XmlHandler.getTagValue(transformNode, "spreadsheetKey");
      this.create = Boolean.parseBoolean(XmlHandler.getTagValue(transformNode, "CREATE"));
      this.append = Boolean.parseBoolean(XmlHandler.getTagValue(transformNode, "APPEND"));
      this.shareEmail = XmlHandler.getTagValue(transformNode, "SHAREEMAIL");
      this.shareDomain = XmlHandler.getTagValue(transformNode, "SHAREDOMAIN");

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform from XML", e);
    }
  }

  /*  @Override
   public void getFields(IRowMeta rowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider) throws HopTransformException {
       try {
            rowMeta.clear(); // Start with a clean slate, eats the input
            for ( int i = 0; i < inputFields.length; i++ ) {
    GoogleSheetsPluginInputFields field = inputFields[i];

    int type = field.getType();
    if ( type == 0 ) { ///ValueMetaInterface.TYPE_NONE
  	type = 2;//ValueMetaInterface.TYPE_STRING;
    }

    try {
  	IValueMeta v = ValueMetaFactory.createValueMeta( field.getName(), type );

  	v.setLength( field.getLength() );
  	v.setPrecision( field.getPrecision() );
  	v.setOrigin( name );
  	v.setConversionMask( field.getFormat() );
  	v.setDecimalSymbol( field.getDecimalSymbol() );
  	v.setGroupingSymbol( field.getGroupSymbol() );
  	v.setCurrencySymbol( field.getCurrencySymbol() );
  	//v.setDateFormatLenient( dateFormatLenient );
  	//v.setDateFormatLocale( dateFormatLocale );
  	v.setTrimType( field.getTrimType() );

  	rowMeta.addValueMeta( v );
    } catch ( Exception e ) {
  	throw new HopTransformException( e );
    }
  }
       } catch (Exception e) {

       }
   }*/

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables space,
      IHopMetadataProvider metadataProvider) {
    if (prev == null || prev.size() == 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Not receiving any fields from previous transforms.",
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              String.format(
                  "Transform is connected to previous one, receiving %1$d fields", prev.size()),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Transform is receiving info from other transforms!",
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "No input received from other transforms.",
              transformMeta));
    }
  }

  @Override
  public GoogleSheetsPluginOutput createTransform(
      TransformMeta transformMeta,
      GoogleSheetsPluginOutputData iTransformData,
      int cnr,
      PipelineMeta pm,
      Pipeline pipeline) {
    return new GoogleSheetsPluginOutput(transformMeta, this, iTransformData, cnr, pm, pipeline);
  }

  @Override
  public GoogleSheetsPluginOutputData getTransformData() {
    return new GoogleSheetsPluginOutputData();
  }

  @Override
  public String getDialogClassName() {
    return GoogleSheetsPluginOutputDialog.class.getName();
  }
  /*public Neo4Jtput createTransform( TransformMeta transformMeta, Neo4JOutputData iTransformData, int cnr, PipelineMeta pipelineMeta, Pipeline disp ) {
    return new Neo4JOutput( transformMeta, this, iTransformData, cnr, pipelineMeta, disp );
  }

  public Neo4JOutputData getTransformData() {
    return new Neo4JOutputData();
  }*/

}
