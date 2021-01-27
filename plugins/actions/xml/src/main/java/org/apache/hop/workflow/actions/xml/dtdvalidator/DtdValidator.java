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

package org.apache.hop.workflow.actions.xml.dtdvalidator;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines a 'dtdvalidator' job entry.
 *
 * @author Samatar Hassan
 * @since 30-04-2007
 */
@Action(
    id = "DTD_VALIDATOR",
    name = "i18n::DTD_VALIDATOR.Name",
    description = "i18n::DTD_VALIDATOR.Description",
    image = "DTD.svg",
    categoryDescription = "i18n::DTD_VALIDATOR.Category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/dtdvalidator.html")
public class DtdValidator extends ActionBase implements Cloneable, IAction {
  private String xmlfilename;
  private String dtdfilename;
  private boolean dtdintern;

  public DtdValidator(String n) {
    super(n, "");
    xmlfilename = null;
    dtdfilename = null;
    dtdintern = false;
  }

  public DtdValidator() {
    this("");
  }

  public Object clone() {
    DtdValidator je = (DtdValidator) super.clone();
    return je;
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer(50);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("xmlfilename", xmlfilename));
    retval.append("      ").append(XmlHandler.addTagValue("dtdfilename", dtdfilename));
    retval.append("      ").append(XmlHandler.addTagValue("dtdintern", dtdintern));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {

    try {
      super.loadXml(entrynode);
      xmlfilename = XmlHandler.getTagValue(entrynode, "xmlfilename");
      dtdfilename = XmlHandler.getTagValue(entrynode, "dtdfilename");
      dtdintern = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "dtdintern"));

    } catch (HopXmlException xe) {
      throw new HopXmlException(
          "Unable to load job entry of type 'DTDvalidator' from XML node", xe);
    }
  }

  public String getRealxmlfilename() {
    return resolve(xmlfilename);
  }

  public String getRealDTDfilename() {
    return resolve(dtdfilename);
  }

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(true);

    String realxmlfilename = getRealxmlfilename();
    String realDTDfilename = getRealDTDfilename();

    // Define a new DTD validator instance
    DtdValidatorUtil validator = new DtdValidatorUtil(log);
    // Set XML filename
    validator.setXMLFilename(realxmlfilename);
    if (dtdintern) {
      // The DTD is intern to XML file
      validator.setInternDTD(true);
    } else {
      // The DTD is extern
      // set the DTD filename
      validator.setDTDFilename(realDTDfilename);
    }
    // Validate the XML file and return the status
    boolean status = validator.validate();
    if (!status) {
      // The XML file is invalid!
      log.logError(validator.getErrorMessage());
      result.setResult(false);
      result.setNrErrors(validator.getNrErrors());
      result.setLogText(validator.getErrorMessage());
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public void setxmlFilename(String filename) {
    this.xmlfilename = filename;
  }

  public String getxmlFilename() {
    return xmlfilename;
  }

  public void setdtdFilename(String filename) {
    this.dtdfilename = filename;
  }

  public String getdtdFilename() {
    return dtdfilename;
  }

  public boolean getDTDIntern() {
    return dtdintern;
  }

  public void setDTDIntern(boolean dtdinternin) {
    this.dtdintern = dtdinternin;
  }

  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if ((!Utils.isEmpty(dtdfilename)) && (!Utils.isEmpty(xmlfilename))) {
      String realXmlFileName = variables.resolve(xmlfilename);
      String realXsdFileName = variables.resolve(dtdfilename);
      ResourceReference reference = new ResourceReference(this);
      reference
          .getEntries()
          .add(new ResourceEntry(realXmlFileName, ResourceEntry.ResourceType.FILE));
      reference
          .getEntries()
          .add(new ResourceEntry(realXsdFileName, ResourceEntry.ResourceType.FILE));
      references.add(reference);
    }
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta jobMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileExistsValidator());
    ActionValidatorUtils.andValidator().validate(this, "dtdfilename", remarks, ctx);
    ActionValidatorUtils.andValidator().validate(this, "xmlFilename", remarks, ctx);
  }
}
