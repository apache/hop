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
 */

package org.apache.hop.workflow.actions.xml.xsdvalidator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.xerces.xni.parser.XMLEntityResolver;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.hop.workflow.action.validator.AbstractFileValidator.putVariableSpace;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.andValidator;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.fileExistsValidator;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.notBlankValidator;
import static org.apache.hop.workflow.action.validator.AndValidator.putValidators;

/** This defines a 'xsdvalidator' job entry. */
@Action(
    id = "XSD_VALIDATOR",
    name = "i18n::XSD_VALIDATOR.Name",
    description = "i18n::XSD_VALIDATOR.Description",
    image = "org/apache/hop/workflow/actions/xml/XSD.svg",
    categoryDescription = "i18n::XSD_VALIDATOR.Category",
    keywords = "i18n::XsdValidator.keyword",
    documentationUrl = "/workflow/actions/xsdvalidator.html")
public class XsdValidator extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = XsdValidator.class; // For Translator

  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION =
      "ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION";
  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT = "true";

  private static final String YES = "Y";

  public static final String SPECIFY_FILENAME = "filename";
  public static final String NO_NEED = "noneed";

  private String xsdSource;
  private String xmlfilename;
  private String xsdfilename;

  private boolean allowExternalEntities;

  public XsdValidator(String n) {
    super(n, "");
    xmlfilename = null;
    xsdfilename = null;
    allowExternalEntities =
        Boolean.valueOf(
            System.getProperties()
                .getProperty(
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION,
                    ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT));
  }

  public XsdValidator() {
    this("");
  }

  @Override
  public Object clone() {
    XsdValidator je = (XsdValidator) super.clone();
    return je;
  }

  @Override
  public String getXml() {
    StringBuffer xml = new StringBuffer(50);

    xml.append(super.getXml());
    xml.append("      ").append(XmlHandler.addTagValue("xsdsource", xsdSource));
    xml.append("      ").append(XmlHandler.addTagValue("xmlfilename", xmlfilename));
    xml.append("      ").append(XmlHandler.addTagValue("xsdfilename", xsdfilename));
    xml.append("      ")
        .append(XmlHandler.addTagValue("allowExternalEntities", allowExternalEntities));

    return xml.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      xsdSource = XmlHandler.getTagValue(entrynode, "xsdsource");
      xmlfilename = XmlHandler.getTagValue(entrynode, "xmlfilename");
      xsdfilename = XmlHandler.getTagValue(entrynode, "xsdfilename");
      allowExternalEntities =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "allowExternalEntities"));

    } catch (HopXmlException xe) {
      throw new HopXmlException(
          "Unable to load job entry of type 'xsdvalidator' from XML node", xe);
    }
  }

  public String getRealxmlfilename() {
    return resolve(getxmlFilename());
  }

  public String getRealxsdfilename() {
    return resolve(getxsdFilename());
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    FileObject xmlfile = null;
    FileObject xsdfile = null;
    Schema schemaXSD;

    validateNonNullFileName(xmlfilename, "ActionXSDValidator.XmlFileNotNull.Label", result);

    try {

      String realxmlfilename = getRealxmlfilename();
      xmlfile = getFile(realxmlfilename);

      SchemaFactory factorytXSDValidator1 =
          SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");

      if (xsdSource.equals(SPECIFY_FILENAME)) {
        validateNonNullFileName(xsdfilename, "ActionXSDValidator.XsdFileNotNull.Label", result);
        String realXsdFileName = getRealxsdfilename();
        xsdfile = getFile(realXsdFileName);
        File xsdFile = new File(HopVfs.getFilename(xsdfile));
        schemaXSD = factorytXSDValidator1.newSchema(xsdFile);

      } else {//not specifying filename
        schemaXSD = factorytXSDValidator1.newSchema();
      }

      Validator xsdValidator = schemaXSD.newValidator();

      if (!isAllowExternalEntities()) {
        xsdValidator.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        xsdValidator.setFeature("http://xml.org/sax/features/external-general-entities", false);
        xsdValidator.setFeature(
            "http://xml.org/sax/features/external-parameter-entities", false);
        xsdValidator.setProperty(
            "http://apache.org/xml/properties/internal/entity-resolver",
            (XMLEntityResolver)
                xmlResourceIdentifier -> {
                  String message =
                      BaseMessages.getString(PKG, "ActionXSDValidator.Error.DisallowedDocType");
                  throw new IOException(message);
                });
      }

      File xmlfiletXSDValidator1 = new File(HopVfs.getFilename(xmlfile));

      Source sourcetXSDValidator1 = new StreamSource(xmlfiletXSDValidator1);

      xsdValidator.validate(sourcetXSDValidator1);

      // Everything is OK
      result.setResult(true);

    } catch (SAXException ex) {
      logError("Error :" + ex.getMessage());
      result.setNrErrors(1);

    } catch (HopFileException | IOException e) {
      logError(BaseMessages.getString(PKG, "ActionXSDValidator.ErrorXSD2.Label") + e.getMessage());
      result.setNrErrors(1);

    } finally {
      try {
        if (xmlfile != null) {
          xmlfile.close();
        }

        if (xsdfile != null) {
          xsdfile.close();
        }

      } catch (IOException e) {
        logError(BaseMessages.getString(PKG, "ActionXSDValidator.ErrorCloseFile.Label") + e.getMessage());
        result.setNrErrors(1);
      }
    }

    return result;
  }

  private void validateNonNullFileName(String filename, String key, Result result) {
    if(StringUtils.isEmpty(filename)){
      logError(BaseMessages.getString(PKG, key));
      result.setNrErrors(1);
    }
  }

  private FileObject getFile(String filename) throws HopFileException, FileSystemException {
    FileObject fileObject = HopVfs.getFileObject(filename);
    if (!fileObject.exists()) {
      logError(
          BaseMessages.getString(PKG, "ActionXSDValidator.FileDoesNotExist1.Label")
              + filename
              + BaseMessages.getString(PKG, "ActionXSDValidator.FileDoesNotExist2.Label"));
    }
    return fileObject;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  public void setxmlFilename(String filename) {
    this.xmlfilename = filename;
  }

  public String getxmlFilename() {
    return xmlfilename;
  }

  public void setxsdFilename(String filename) {
    this.xsdfilename = filename;
  }

  public String getxsdFilename() {
    return xsdfilename;
  }

  public boolean isAllowExternalEntities() {
    return allowExternalEntities;
  }

  public String getXsdSource() {
    return xsdSource;
  }

  public void setXsdSource(String xsdSource) {
    this.xsdSource = xsdSource;
  }

  public void setAllowExternalEntities(boolean allowExternalEntities) {
    this.allowExternalEntities = allowExternalEntities;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if ((!Utils.isEmpty(xsdfilename)) && (!Utils.isEmpty(xmlfilename))) {
      String realXmlFileName = resolve(xmlfilename);
      String realXsdFileName = resolve(xsdfilename);
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
    putVariableSpace(ctx, getVariables());
    putValidators(ctx, notBlankValidator(), fileExistsValidator());
    andValidator().validate(this, "xsdFilename", remarks, ctx);
    andValidator().validate(this, "xmlFilename", remarks, ctx);
  }
}
