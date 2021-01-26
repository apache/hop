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

package org.apache.hop.workflow.actions.xml.xsdvalidator;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
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

/**
 * This defines a 'xsdvalidator' job entry.
 *
 * @author Samatar Hassan
 * @since 30-04-2007
 */
@Action(
    id = "XSD_VALIDATOR",
    name = "i18n::XSD_VALIDATOR.Name",
    description = "i18n::XSD_VALIDATOR.Description",
    image = "org/apache/hop/workflow/actions/xml/XSD.svg",
    categoryDescription = "i18n::XSD_VALIDATOR.Category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/xsdvalidator.html")
public class XsdValidator extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = XsdValidator.class; // For Translator

  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION =
      "ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION";
  public static final String ALLOW_EXTERNAL_ENTITIES_FOR_XSD_VALIDATION_DEFAULT = "true";

  private static final String YES = "Y";

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

  public Object clone() {
    XsdValidator je = (XsdValidator) super.clone();
    return je;
  }

  public String getXml() {
    StringBuffer xml = new StringBuffer(50);

    xml.append(super.getXml());
    xml.append("      ").append(XmlHandler.addTagValue("xmlfilename", xmlfilename));
    xml.append("      ").append(XmlHandler.addTagValue("xsdfilename", xsdfilename));
    xml.append("      ")
        .append(XmlHandler.addTagValue("allowExternalEntities", allowExternalEntities));

    return xml.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
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

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    String realxmlfilename = getRealxmlfilename();
    String realxsdfilename = getRealxsdfilename();

    FileObject xmlfile = null;
    FileObject xsdfile = null;

    try {

      if (xmlfilename != null && xsdfilename != null) {
        xmlfile = HopVfs.getFileObject(realxmlfilename);
        xsdfile = HopVfs.getFileObject(realxsdfilename);

        if (xmlfile.exists() && xsdfile.exists()) {

          SchemaFactory factorytXSDValidator_1 =
              SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");

          // Get XSD File
          File XSDFile = new File(HopVfs.getFilename(xsdfile));
          Schema SchematXSD = factorytXSDValidator_1.newSchema(XSDFile);

          Validator xsdValidator = SchematXSD.newValidator();

          // Prevent against XML Entity Expansion (XEE) attacks.
          // https://www.owasp.org/index.php/XML_Security_Cheat_Sheet#XML_Entity_Expansion
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
                          BaseMessages.getString(
                              PKG, "JobEntryXSDValidator.Error.DisallowedDocType");
                      throw new IOException(message);
                    });
          }

          // Get XML File
          File xmlfiletXSDValidator_1 = new File(HopVfs.getFilename(xmlfile));

          Source sourcetXSDValidator_1 = new StreamSource(xmlfiletXSDValidator_1);

          xsdValidator.validate(sourcetXSDValidator_1);

          // Everything is OK
          result.setResult(true);

        } else {

          if (!xmlfile.exists()) {
            logError(
                BaseMessages.getString(PKG, "JobEntryXSDValidator.FileDoesNotExist1.Label")
                    + realxmlfilename
                    + BaseMessages.getString(PKG, "JobEntryXSDValidator.FileDoesNotExist2.Label"));
          }
          if (!xsdfile.exists()) {
            logError(
                BaseMessages.getString(PKG, "JobEntryXSDValidator.FileDoesNotExist1.Label")
                    + realxsdfilename
                    + BaseMessages.getString(PKG, "JobEntryXSDValidator.FileDoesNotExist2.Label"));
          }
          result.setResult(false);
          result.setNrErrors(1);
        }

      } else {
        logError(BaseMessages.getString(PKG, "JobEntryXSDValidator.AllFilesNotNull.Label"));
        result.setResult(false);
        result.setNrErrors(1);
      }

    } catch (SAXException ex) {
      logError("Error :" + ex.getMessage());
    } catch (Exception e) {

      logError(
          BaseMessages.getString(PKG, "JobEntryXSDValidator.ErrorXSDValidator.Label")
              + BaseMessages.getString(PKG, "JobEntryXSDValidator.ErrorXML1.Label")
              + realxmlfilename
              + BaseMessages.getString(PKG, "JobEntryXSDValidator.ErrorXML2.Label")
              + BaseMessages.getString(PKG, "JobEntryXSDValidator.ErrorXSD1.Label")
              + realxsdfilename
              + BaseMessages.getString(PKG, "JobEntryXSDValidator.ErrorXSD2.Label")
              + e.getMessage());
      result.setResult(false);
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
        // Ignore errors
      }
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

  public void setxsdFilename(String filename) {
    this.xsdfilename = filename;
  }

  public String getxsdFilename() {
    return xsdfilename;
  }

  public boolean isAllowExternalEntities() {
    return allowExternalEntities;
  }

  public void setAllowExternalEntities(boolean allowExternalEntities) {
    this.allowExternalEntities = allowExternalEntities;
  }

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
