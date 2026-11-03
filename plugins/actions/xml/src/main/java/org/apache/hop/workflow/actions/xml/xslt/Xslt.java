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

package org.apache.hop.workflow.actions.xml.xslt;

import static org.apache.hop.workflow.action.validator.AbstractFileValidator.putVariableSpace;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.andValidator;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.fileExistsValidator;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.notBlankValidator;
import static org.apache.hop.workflow.action.validator.AndValidator.putValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.HopVfsUriResolver;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.xml.ILegacyXml;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

/** This defines a 'xslt' job entry. */
@Getter
@Setter
@Action(
    id = "XSLT",
    name = "i18n::XSLT.Name",
    description = "i18n::XSLT.Description",
    image = "XSLT.svg",
    categoryDescription = "i18n::XSLT.Category",
    keywords = "i18n::Xslt.keyword",
    documentationUrl = "/workflow/actions/xslt.html")
public class Xslt extends ActionBase implements Cloneable, IAction, ILegacyXml {
  private static final Class<?> PKG = Xslt.class;

  public static final String FACTORY_JAXP = "JAXP";
  public static final String FACTORY_SAXON = "SAXON";
  public static final String CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_1_LABEL =
      "ActionXSLT.OuputFileExists1.Label";
  public static final String CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_2_LABEL =
      "ActionXSLT.OuputFileExists2.Label";

  @HopMetadataProperty(key = "xmlfilename")
  private String xmlFilename;

  @HopMetadataProperty(key = "xslfilename")
  private String xslFilename;

  @HopMetadataProperty(key = "outputfilename")
  private String outputFilename;

  @HopMetadataProperty private int ifFileExists;

  @HopMetadataProperty(key = "addfiletoresult")
  private boolean addFileToResult;

  /** Stored under the legacy XML tag {@code xsltfactory}. */
  @HopMetadataProperty(key = "xsltfactory")
  private String transformerFactory;

  @HopMetadataProperty(key = "filenamesfromprevious")
  private boolean filenamesFromPrevious;

  @HopMetadataProperty(key = "parameter", groupKey = "parameters")
  private List<Parameter> parameters;

  @HopMetadataProperty(key = "outputproperty", groupKey = "outputproperties")
  private List<OutputProperty> outputPropertyList;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private int nrParams;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private String[] nameOfParams;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private String[] valueOfParams;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private boolean useParameters;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private Properties transformerOutputProperties;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private boolean setOutputProperties;

  public Xslt(String n) {
    super(n, "");
    xmlFilename = null;
    xslFilename = null;
    outputFilename = null;
    ifFileExists = 1;
    addFileToResult = false;
    filenamesFromPrevious = false;
    transformerFactory = FACTORY_JAXP;
    parameters = new ArrayList<>();
    outputPropertyList = new ArrayList<>();
  }

  public Xslt() {
    this("");
  }

  /**
   * Before the XML cleanup (57bd0dcdab), {@code getXml()} / {@code loadXml()} used the tag {@code
   * iffileexists}. Annotation serialization writes {@code ifFileExists}; workflows saved before the
   * cleanup still carry the old tag and would otherwise fall back to the default (1, do nothing).
   */
  @Override
  public void convertLegacyXml(Node node) {
    if (XmlHandler.getTagValue(node, "ifFileExists") == null) {
      String legacy = XmlHandler.getTagValue(node, "iffileexists");
      if (legacy != null) {
        ifFileExists = Const.toInt(legacy, 1);
      }
    }
  }

  @Override
  public Object clone() {
    Xslt je = (Xslt) super.clone();
    je.parameters = new ArrayList<>();
    for (Parameter parameter : parameters) {
      je.parameters.add(new Parameter(parameter));
    }
    je.outputPropertyList = new ArrayList<>();
    for (OutputProperty outputProperty : outputPropertyList) {
      je.outputPropertyList.add(new OutputProperty(outputProperty));
    }
    return je;
  }

  public String getRealXmlFilename() {
    return resolve(xmlFilename);
  }

  public String getRealXslFilename() {
    return resolve(xslFilename);
  }

  public String getRealOutputFilename() {
    return resolve(outputFilename);
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    int nrErrors = 0;
    int nrSuccess = 0;

    // Check output parameters
    int nrOutputProps = outputPropertyList == null ? 0 : outputPropertyList.size();
    if (nrOutputProps > 0) {
      transformerOutputProperties = new Properties();
      for (int i = 0; i < nrOutputProps; i++) {
        OutputProperty outputProperty = outputPropertyList.get(i);
        transformerOutputProperties.put(
            outputProperty.getName(), resolve(outputProperty.getValue()));
      }
      setOutputProperties = true;
    }

    // Check parameters
    nrParams = parameters == null ? 0 : parameters.size();
    if (nrParams > 0) {
      nameOfParams = new String[nrParams];
      valueOfParams = new String[nrParams];
      for (int i = 0; i < nrParams; i++) {
        Parameter parameter = parameters.get(i);
        String name = resolve(parameter.getName());
        String value = resolve(parameter.getField());
        if (Utils.isEmpty(value)) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "Xslt.Exception.ParameterFieldMissing", name, i));
        }
        nameOfParams[i] = name;
        valueOfParams[i] = value;
      }
      useParameters = true;
    }

    List<RowMetaAndData> rows = result.getRows();
    if (isFilenamesFromPrevious() && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionXSLT.Log.ArgFromPrevious.Found", (rows != null ? rows.size() : 0) + ""));
    }

    if (isFilenamesFromPrevious() && rows != null) {
      // Copy the input row to the (command line) arguments
      RowMetaAndData resultRow = null;
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        resultRow = rows.get(iteration);

        // Get filenames (xml, xsl, output filename)
        String xmlfilenamePrevious = resultRow.getString(0, null);
        String xslfilenamePrevious = resultRow.getString(1, null);
        String ouputfilenamePrevious = resultRow.getString(2, null);

        if (!Utils.isEmpty(xmlfilenamePrevious)
            && !Utils.isEmpty(xslfilenamePrevious)
            && !Utils.isEmpty(ouputfilenamePrevious)) {
          if (processOneXMLFile(
              xmlfilenamePrevious,
              xslfilenamePrevious,
              ouputfilenamePrevious,
              result,
              parentWorkflow)) {
            nrSuccess++;
          } else {
            nrErrors++;
          }
        } else {
          // We failed!
          logError(BaseMessages.getString(PKG, "ActionXSLT.AllFilesNotNull.Label"));
          nrErrors++;
        }
      }
    } else {
      String realXmlFilename = getRealXmlFilename();
      String realXslFilename = getRealXslFilename();
      String realOutputFilename = getRealOutputFilename();
      if (!Utils.isEmpty(realXmlFilename)
          && !Utils.isEmpty(realXslFilename)
          && !Utils.isEmpty(realOutputFilename)) {
        if (processOneXMLFile(
            realXmlFilename, realXslFilename, realOutputFilename, result, parentWorkflow)) {
          nrSuccess++;
        } else {
          nrErrors++;
        }
      } else {
        // We failed!
        logError(BaseMessages.getString(PKG, "ActionXSLT.AllFilesNotNull.Label"));
        nrErrors++;
      }
    }

    result.setResult(nrErrors == 0);
    result.setNrErrors(nrErrors);
    result.setNrLinesWritten(nrSuccess);

    return result;
  }

  private boolean processOneXMLFile(
      String xmlfilename,
      String xslfilename,
      String outputfilename,
      Result result,
      IWorkflowEngine parentWorkflow) {
    boolean retval = false;
    FileObject xmlfile = null;
    FileObject xslfile = null;
    FileObject outputfile = null;

    try {
      xmlfile = HopVfs.getFileObject(xmlfilename);
      xslfile = HopVfs.getFileObject(xslfilename);
      outputfile = HopVfs.getFileObject(outputfilename);

      if (xmlfile.exists() && xslfile.exists()) {
        if (outputfile.exists() && ifFileExists == 2) {
          // Output file exists
          // User want to fail
          logError(
              BaseMessages.getString(PKG, CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_1_LABEL)
                  + outputfilename
                  + BaseMessages.getString(PKG, CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_2_LABEL));
          return retval;

        } else if (outputfile.exists() && ifFileExists == 1) {
          // Do nothing
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(PKG, CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_1_LABEL)
                    + outputfilename
                    + BaseMessages.getString(PKG, CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_2_LABEL));
          }
          retval = true;
          return retval;

        } else {
          if (outputfile.exists() && ifFileExists == 0) {
            // the output file exists and user want to create new one with unique name
            // Format Date

            // Try to clean filename (without wildcard)
            String wildcard = outputfilename.substring(outputfilename.length() - 4);
            if (wildcard.substring(0, 1).equals(".")) {
              // Find wildcard
              outputfilename =
                  outputfilename.substring(0, outputfilename.length() - 4)
                      + "_"
                      + StringUtil.getFormattedDateTimeNow(true)
                      + wildcard;
            } else {
              // did not find wildcard
              outputfilename = outputfilename + "_" + StringUtil.getFormattedDateTimeNow(true);
            }
            // Re-resolve so the transformation writes to the unique name, not the original file
            FileObject previousOutput = outputfile;
            outputfile = HopVfs.getFileObject(outputfilename);
            try {
              previousOutput.close();
            } catch (IOException e) {
              logError("Unable to close file", e);
            }
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(PKG, CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_1_LABEL)
                      + outputfilename
                      + BaseMessages.getString(PKG, CONST_ACTION_XSLT_OUPUT_FILE_EXISTS_2_LABEL));
              logDebug(
                  BaseMessages.getString(PKG, "ActionXSLT.OuputFileNameChange1.Label")
                      + outputfilename
                      + BaseMessages.getString(PKG, "ActionXSLT.OuputFileNameChange2.Label"));
            }
          }

          // Create transformer factory
          TransformerFactory factory = TransformerFactory.newInstance();

          if (FACTORY_SAXON.equals(transformerFactory)) {
            // Set the TransformerFactory to the SAXON implementation.
            factory = new net.sf.saxon.TransformerFactoryImpl();
          }
          // Resolve includes, imports and document() through Hop VFS
          factory.setURIResolver(new HopVfsUriResolver(this));

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionXSL.Log.TransformerFactoryInfos"),
                BaseMessages.getString(
                    PKG, "ActionXSL.Log.TransformerFactory", factory.getClass().getName()));
          }

          InputStream xslInputStream = HopVfs.getInputStream(xslfile);
          InputStream xmlInputStream = HopVfs.getInputStream(xmlfile);
          OutputStream os = null;
          try {
            // Use the factory to create a template containing the xsl file. Its URI resolves
            // relative xsl:include and xsl:import references against the stylesheet's folder.
            Templates template =
                factory.newTemplates(new StreamSource(xslInputStream, xslfile.getName().getURI()));

            // Use the template to create a transformer
            Transformer xformer = template.newTransformer();
            xformer.setURIResolver(factory.getURIResolver());

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionXSL.Log.TransformerClassInfos"),
                  BaseMessages.getString(
                      PKG, "ActionXSL.Log.TransformerClass", xformer.getClass().getName()));
            }

            // Do we need to set output properties?
            if (setOutputProperties) {
              xformer.setOutputProperties(transformerOutputProperties);
            }

            // Do we need to pass parameters?
            if (useParameters) {
              for (int i = 0; i < nrParams; i++) {
                xformer.setParameter(nameOfParams[i], valueOfParams[i]);
              }
            }

            // Prepare the input and output files
            Source source = new StreamSource(xmlInputStream, xmlfile.getName().getURI());
            os = HopVfs.getOutputStream(outputfile, false);
            StreamResult resultat = new StreamResult(os);

            // Apply the xsl file to the source file and write the result to the output file
            xformer.transform(source, resultat);

            if (isAddFileToResult()) {
              // Add output filename to output files
              ResultFile resultFile =
                  new ResultFile(
                      ResultFile.FILE_TYPE_GENERAL,
                      HopVfs.getFileObject(outputfilename),
                      parentWorkflow.getWorkflowName(),
                      toString());
              result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
            }

            // Everything is OK
            retval = true;
          } finally {
            try {
              xslInputStream.close();
            } catch (IOException ignored) {
              // ignore IO Exception on close
            }
            try {
              xmlInputStream.close();
            } catch (IOException ignored) {
              // ignore IO Exception on close
            }
            try {
              if (os != null) {
                os.close();
              }
            } catch (IOException ignored) {
              // ignore IO Exception on close
            }
          }
        }
      } else {

        if (!xmlfile.exists()) {
          logError(
              BaseMessages.getString(PKG, "ActionXSLT.FileDoesNotExist1.Label")
                  + xmlfilename
                  + BaseMessages.getString(PKG, "ActionXSLT.FileDoesNotExist2.Label"));
        }
        if (!xslfile.exists()) {
          logError(
              BaseMessages.getString(PKG, "ActionXSLT.FileDoesNotExist1.Label")
                  + xslfilename
                  + BaseMessages.getString(PKG, "ActionXSLT.FileDoesNotExist2.Label"));
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionXSLT.ErrorXLST.Label")
              + BaseMessages.getString(PKG, "ActionXSLT.ErrorXLSTXML1.Label")
              + xmlfilename
              + BaseMessages.getString(PKG, "ActionXSLT.ErrorXLSTXML2.Label")
              + BaseMessages.getString(PKG, "ActionXSLT.ErrorXLSTXSL1.Label")
              + xslfilename
              + BaseMessages.getString(PKG, "ActionXSLT.ErrorXLSTXSL2.Label")
              + e.getMessage());
    } finally {
      closeFileStream(xmlfile, xslfile, outputfile);
    }

    return retval;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if ((!Utils.isEmpty(xslFilename)) && (!Utils.isEmpty(xmlFilename))) {
      String realXmlFileName = variables.resolve(xmlFilename);
      String realXslFileName = variables.resolve(xslFilename);
      ResourceReference reference = new ResourceReference(this);
      reference
          .getEntries()
          .add(new ResourceEntry(realXmlFileName, ResourceEntry.ResourceType.FILE));
      reference
          .getEntries()
          .add(new ResourceEntry(realXslFileName, ResourceEntry.ResourceType.FILE));
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
    andValidator().validate(this, "xmlFilename", remarks, ctx);
    andValidator().validate(this, "xslFilename", remarks, ctx);

    andValidator().validate(this, "outputFilename", remarks, putValidators(notBlankValidator()));
  }

  @Getter
  @Setter
  public static class Parameter {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "field")
    private String field;

    public Parameter() {}

    public Parameter(Parameter parameter) {
      this.name = parameter.name;
      this.field = parameter.field;
    }

    public Parameter(String name, String field) {
      this.name = name;
      this.field = field;
    }
  }

  @Getter
  @Setter
  public static class OutputProperty {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "value")
    private String value;

    public OutputProperty() {}

    public OutputProperty(OutputProperty outputProperty) {
      this.name = outputProperty.name;
      this.value = outputProperty.value;
    }

    public OutputProperty(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  private void closeFileStream(FileObject xmlFile, FileObject xslFile, FileObject outputFile) {
    try {
      if (xmlFile != null) {
        xmlFile.close();
      }

      if (xslFile != null) {
        xslFile.close();
      }
      if (outputFile != null) {
        outputFile.close();
      }
    } catch (IOException e) {
      logError("Unable to close file", e);
    }
  }
}
