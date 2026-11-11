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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.xerces.xni.parser.XMLEntityResolver;
import org.xml.sax.SAXException;

/**
 * Executes a xsd validator on the values in the input stream. New fields were calculated values can
 * then be put on the output stream.
 */
public class XsdValidator extends BaseTransform<XsdValidatorMeta, XsdValidatorData> {
  private static final Class<?> PKG = XsdValidatorMeta.class;

  static final String JAXP_SCHEMA_LANGUAGE =
      "http://java.sun.com/xml/jaxp/properties/schemaLanguage";

  static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";

  public XsdValidator(
      TransformMeta transformMeta,
      XsdValidatorMeta meta,
      XsdValidatorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline trans) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, trans);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();

    if (row == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Check if XML stream is given
      if (meta.getXmlStream() != null) {
        validXmlStreamField();

        // Let's check that Result Field is given
        if (meta.getResultFieldName() == null) {
          // Result field is missing !
          logError(BaseMessages.getString(PKG, "XsdValidator.Log.ErrorResultFieldMissing"));
          throw new HopTransformException(
              BaseMessages.getString(PKG, "XsdValidator.Exception.ErrorResultFieldMissing"));
        }

        // Is XSD file is provided?
        if (meta.getXsdSource().equals(meta.SPECIFY_FILENAME)) {
          validXsdFileName();
        }

        // Is XSD field is provided?
        if (meta.getXsdSource().equals(meta.SPECIFY_FIELDNAME)) {
          validXsdField();
        }

      } else {
        // XML stream field is missing !
        logError(BaseMessages.getString(PKG, "XsdValidator.Log.Error.XmlStreamFieldMissing"));
        throw new HopTransformException(
            BaseMessages.getString(PKG, "XsdValidator.Exception.XmlStreamFieldMissing"));
      }
    }

    try {

      boolean isvalid = false;

      // XSD filename
      String xsdfilename = null;

      String validationmsg = null;

      if (meta.getXsdSource().equals(meta.SPECIFY_FILENAME)) {
        xsdfilename = resolve(meta.getXsdFilename());
      } else if (meta.getXsdSource().equals(meta.SPECIFY_FIELDNAME)) {
        // Get the XSD field value
        xsdfilename = getInputRowMeta().getString(row, data.xsdindex);
      }

      // A compiled schema is kept per copy, by XSD file name. Only resolve the file when the
      // schema still has to be compiled.
      Schema cachedSchema = getCachedSchema(xsdfilename);
      FileObject xsdfile =
          cachedSchema == null && StringUtils.isNotEmpty(xsdfilename)
              ? HopVfs.getFileObject(xsdfilename, variables)
              : null;

      try {

        // Get XML stream
        Source sourceXML = getSourceXML(getInputRowMeta().getString(row, data.xmlindex));

        // The compiled schema: from the cache, or compiled now and kept
        Schema schematXSD =
            cachedSchema != null ? cachedSchema : compileSchema(xsdfilename, xsdfile);

        // The validator of this schema, configured once
        Validator xsdValidator = getValidator(schematXSD);

        // Validate XML / XSD
        try {
          xsdValidator.validate(sourceXML);
        } catch (SAXException | IOException e) {
          // Don't reuse a validator that failed; the next row gets a new one.
          data.validators.remove(schematXSD);
          throw e;
        }

        isvalid = true;

      } catch (SAXException ex) {
        validationmsg = ex.getMessage();
      } catch (IOException ex) {
        validationmsg = ex.getMessage();
      } finally {
        try {
          if (xsdfile != null) {
            xsdfile.close();
          }
        } catch (IOException e) {
          // Ignore errors
        }
      }

      Object[] outputRowData = null;
      Object[] outputRowData2 = null;

      if (meta.isOutputStringField()) {
        // Output type=String
        if (isvalid) {
          outputRowData =
              RowDataUtil.addValueData(
                  row, getInputRowMeta().size(), resolve(meta.getIfXmlValid()));
        } else {
          outputRowData =
              RowDataUtil.addValueData(
                  row, getInputRowMeta().size(), resolve(meta.getIfXmlInvalid()));
        }
      } else {
        outputRowData = RowDataUtil.addValueData(row, getInputRowMeta().size(), isvalid);
      }

      if (meta.isAddValidationMessage()) {
        outputRowData2 =
            RowDataUtil.addValueData(outputRowData, getInputRowMeta().size() + 1, validationmsg);
      } else {
        outputRowData2 = outputRowData;
      }

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "XsdValidator.Log.ReadRow")
                + " "
                + getInputRowMeta().getString(row));
      }

      // add new values to the row.
      putRow(data.outputRowMeta, outputRowData2); // copy row to output rowset(s)
    } catch (HopException e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), row, 1, errorMessage, null, "XSD001");
      } else {
        logError(
            BaseMessages.getString(PKG, "XsdValidator.ErrorProcesing" + " : " + e.getMessage()));
        throw new HopTransformException(
            BaseMessages.getString(PKG, "XsdValidator.ErrorProcesing"), e);
      }
    }

    return true;
  }

  /**
   * The schema factory of this copy, created on first use. The factory resolves the schema document
   * itself, so it needs the same external-access restrictions as the validator it produces. Honour
   * the transform's own opt-in so a pipeline that deliberately relies on remote schemas keeps
   * working. A SchemaFactory is not thread-safe, which is why it is kept per copy.
   */
  private SchemaFactory getSchemaFactory() throws SAXException {
    if (data.schemaFactory == null) {
      data.schemaFactory =
          meta.isAllowExternalEntities()
              ? SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
              : XmlParserFactoryProducer.createSecureSchemaFactory(
                  XMLConstants.W3C_XML_SCHEMA_NS_URI);
    }
    return data.schemaFactory;
  }

  /** The schema compiled earlier for this XSD file name, or null. */
  private Schema getCachedSchema(String xsdfilename) {
    if (StringUtils.isEmpty(xsdfilename)) {
      return data.schemaFromDocuments;
    }
    return data.schemas.get(xsdfilename);
  }

  /**
   * Compiles the schema and keeps it for the next rows. A schema that fails to compile is not kept,
   * so every row that uses it reports the error.
   */
  private Schema compileSchema(String xsdfilename, FileObject xsdfile)
      throws SAXException, HopTransformException, FileSystemException {
    if (xsdfile == null) {
      Schema schema = getXsdSchema(xsdfilename, getSchemaFactory());
      data.schemaFromDocuments = schema;
      return schema;
    }
    Schema schema = getXsdSchemaFromFile(xsdfile, getSchemaFactory());
    data.schemas.put(xsdfilename, schema);
    return schema;
  }

  /**
   * The validator of this schema for this copy, configured once. A Validator is not thread-safe,
   * which is why it is kept per copy. reset() is not used: it would drop the features set here.
   *
   * <p>The schema without an XSD file is the exception: its validator keeps the schemas it loaded
   * from the location hints of earlier documents and would validate later documents against them,
   * whatever their own hints say. That schema gets a new validator for every row.
   */
  private Validator getValidator(Schema schema) throws SAXException {
    Validator xsdValidator = data.validators.get(schema);
    if (xsdValidator != null) {
      return xsdValidator;
    }
    xsdValidator = schema.newValidator();

    // Prevent against XML Entity Expansion (XEE) attacks.
    // https://www.owasp.org/index.php/XML_Security_Cheat_Sheet#XML_Entity_Expansion
    if (!meta.isAllowExternalEntities()) {
      xsdValidator.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      xsdValidator.setFeature("http://xml.org/sax/features/external-general-entities", false);
      xsdValidator.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      xsdValidator.setProperty(
          "http://apache.org/xml/properties/internal/entity-resolver",
          (XMLEntityResolver)
              xmlResourceIdentifier -> {
                String message =
                    BaseMessages.getString(PKG, "XsdValidator.Exception.DisallowedDocType");
                throw new IOException(message);
              });
    }
    if (schema != data.schemaFromDocuments) {
      data.validators.put(schema, xsdValidator);
    }
    return xsdValidator;
  }

  private Schema getXsdSchema(String xsdfilename, SchemaFactory factoryXSDValidator)
      throws SAXException, HopTransformException {
    Schema schematXSD;
    if (meta.getXsdSource().equals(meta.NO_NEED)) {
      // ---Some documents specify the schema they expect to be validated against,
      // ---typically using xsi:noNamespaceSchemaLocation and/or xsi:schemaLocation attributes
      schematXSD = factoryXSDValidator.newSchema();
    } else {
      // we should not get here as anything entered in that does not look like
      // a url should be made a FileObject.
      throw new HopTransformException(
          BaseMessages.getString(PKG, "XsdValidator.Exception.CannotCreateSchema", xsdfilename));
    }
    return schematXSD;
  }

  private Schema getXsdSchemaFromFile(FileObject xsdfile, SchemaFactory factoryXSDValidator)
      throws SAXException, FileSystemException {
    Schema schematXSD;
    if (xsdfile.getName().getURI().contains("ram:///")) {
      schematXSD =
          factoryXSDValidator.newSchema(new StreamSource(xsdfile.getContent().getInputStream()));
    } else {
      schematXSD = factoryXSDValidator.newSchema(new File(HopVfs.getFilename(xsdfile)));
    }
    return schematXSD;
  }

  private void validXmlStreamField() throws HopTransformException {
    // Try to get XML Field index
    data.xmlindex = getInputRowMeta().indexOfValue(meta.getXmlStream());
    // Let's check the Field
    if (data.xmlindex < 0) {
      // The field is unreachable !
      logError(
          BaseMessages.getString(PKG, "XsdValidator.Log.ErrorFindingField")
              + "["
              + meta.getXmlStream()
              + "]");
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "XsdValidator.Exception.CouldnotFindField", meta.getXmlStream()));
    }
  }

  private void validXsdField() throws HopTransformException {
    if (meta.getXsdDefinedField() == null) {
      logError(BaseMessages.getString(PKG, "XsdValidator.Log.Error.XSDFieldMissing"));
      throw new HopTransformException(
          BaseMessages.getString(PKG, "XsdValidator.Exception.XSDFieldMissing"));
    } else {
      // Let's check if the XSD field exist
      // Try to get XML Field index
      data.xsdindex = getInputRowMeta().indexOfValue(meta.getXsdDefinedField());

      if (data.xsdindex < 0) {
        // The field is unreachable !
        logError(
            BaseMessages.getString(
                PKG, "XsdValidator.Log.ErrorFindingXSDField", meta.getXsdDefinedField()));
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "XsdValidator.Exception.ErrorFindingXSDField", meta.getXsdDefinedField()));
      }
    }
  }

  private void validXsdFileName() throws HopTransformException {
    if (meta.getXsdFilename() == null) {
      logError(BaseMessages.getString(PKG, "XsdValidator.Log.ErrorXSDFileMissing"));
      throw new HopTransformException(
          BaseMessages.getString(PKG, "XsdValidator.Exception.ErrorXSDFileMissing"));
    } else {
      // Is XSD file exists ?
      FileObject xsdfile = null;
      try {
        xsdfile = HopVfs.getFileObject(resolve(meta.getXsdFilename()), variables);
        if (!xsdfile.exists()) {
          logError(BaseMessages.getString(PKG, "XsdValidator.Log.Error.XSDFileNotExists"));
          throw new HopTransformException(
              BaseMessages.getString(PKG, "XsdValidator.Exception.XSDFileNotExists"));
        }

      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "XsdValidator.Log.Error.GettingXSDFile"));
        throw new HopTransformException(
            BaseMessages.getString(PKG, "XsdValidator.Exception.GettingXSDFile"));
      } finally {
        try {
          if (xsdfile != null) {
            xsdfile.close();
          }
        } catch (IOException e) {
          // Ignore errors
        }
      }
    }
  }

  private Source getSourceXML(String xmlFieldvalue)
      throws HopFileException, FileSystemException, HopTransformException {
    Source sourceXML = new StreamSource(new StringReader(xmlFieldvalue));

    if (meta.isXmlSourceFile()) {

      // We deal with XML file
      // Get XML File
      FileObject xmlfileValidator = HopVfs.getFileObject(xmlFieldvalue, variables);
      if (xmlfileValidator == null || !xmlfileValidator.exists()) {
        logError(
            BaseMessages.getString(PKG, "XsdValidator.Log.Error.XMLfileMissing", xmlFieldvalue));
        throw new HopTransformException(
            BaseMessages.getString(PKG, "XsdValidator.Exception.XMLfileMissing", xmlFieldvalue));
      }
      sourceXML = new StreamSource(xmlfileValidator.getContent().getInputStream());
    }
    return sourceXML;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // Add init code here.
      return true;
    }
    return false;
  }

  @Override
  public void dispose() {

    super.dispose();
  }
}
