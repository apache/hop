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

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
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

@Getter
@Setter
/** This defines a 'dtdvalidator' job entry. */
@Action(
    id = "DTD_VALIDATOR",
    name = "i18n::DTD_VALIDATOR.Name",
    description = "i18n::DTD_VALIDATOR.Description",
    image = "DTD.svg",
    categoryDescription = "i18n::DTD_VALIDATOR.Category",
    keywords = "i18n::DtdValidator.keyword",
    documentationUrl = "/workflow/actions/dtdvalidator.html")
public class DtdValidator extends ActionBase implements Cloneable, IAction {

  @HopMetadataProperty(key = "xmlfilename")
  private String xmlFilename;

  @HopMetadataProperty(key = "dtdfilename")
  private String dtdFilename;

  @HopMetadataProperty(key = "dtdintern")
  private String dtdIntern;

  public DtdValidator(String n) {
    super(n, "");
    xmlFilename = null;
    dtdFilename = null;
    dtdIntern = "N";
  }

  public DtdValidator() {
    this("");
  }

  @Override
  public Object clone() {
    DtdValidator je = (DtdValidator) super.clone();
    return je;
  }

  public String getRealxmlfilename() {
    return resolve(xmlFilename);
  }

  public String getRealDTDfilename() {
    return resolve(dtdFilename);
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(true);

    String realxmlfilename = getRealxmlfilename();
    String realDTDfilename = getRealDTDfilename();

    // Define a new DTD validator instance
    DtdValidatorUtil validator = new DtdValidatorUtil(getLogChannel());
    // Set XML filename
    validator.setXMLFilename(realxmlfilename);
    if ("Y".equals(dtdIntern)) {
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
      logError(validator.getErrorMessage());
      result.setResult(false);
      result.setNrErrors(validator.getNrErrors());
      result.setLogText(validator.getErrorMessage());
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if ((!Utils.isEmpty(dtdFilename)) && (!Utils.isEmpty(xmlFilename))) {
      String realXmlFileName = variables.resolve(xmlFilename);
      String realXsdFileName = variables.resolve(dtdFilename);
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
    ActionValidatorUtils.andValidator().validate(this, dtdFilename, remarks, ctx);
    ActionValidatorUtils.andValidator().validate(this, xmlFilename, remarks, ctx);
  }
}
