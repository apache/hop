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

package org.apache.hop.schema;

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXParseException;

/** Validates XML files or strings against generated XML schemas (XSD). */
public class HopXmlSchemaValidator {

  private HopXmlSchemaValidator() {}

  /**
   * Compiles an XSD schema string into a JAXP Schema object.
   *
   * @param xsdContent XSD schema definition
   * @return compiled Schema
   * @throws HopException if compilation fails
   */
  public static Schema compileSchema(String xsdContent) throws HopException {
    try {
      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      return schemaFactory.newSchema(new StreamSource(new StringReader(xsdContent)));
    } catch (Exception e) {
      throw new HopException("Failed to compile XML schema: " + e.getMessage(), e);
    }
  }

  /**
   * Validates XML content against a compiled Schema, returning any errors.
   *
   * @param xmlContent XML string
   * @param schema compiled Schema
   * @return list of validation error messages (empty if valid)
   */
  public static List<String> validateXml(String xmlContent, Schema schema) {
    List<String> errors = new ArrayList<>();
    try {
      Validator validator = schema.newValidator();
      validator.setErrorHandler(
          new ErrorHandler() {
            @Override
            public void warning(SAXParseException exception) {}

            @Override
            public void error(SAXParseException exception) {
              errors.add(
                  "Line "
                      + exception.getLineNumber()
                      + ", Col "
                      + exception.getColumnNumber()
                      + ": "
                      + exception.getMessage());
            }

            @Override
            public void fatalError(SAXParseException exception) {
              errors.add(
                  "Fatal: Line "
                      + exception.getLineNumber()
                      + ", Col "
                      + exception.getColumnNumber()
                      + ": "
                      + exception.getMessage());
            }
          });
      validator.validate(new StreamSource(new StringReader(xmlContent)));
    } catch (Exception e) {
      errors.add("Validation error: " + e.getMessage());
    }
    return errors;
  }

  /**
   * Validates XML content against an XSD string, returning any errors.
   *
   * @param xmlContent XML string
   * @param xsdContent XSD string
   * @return list of validation error messages (empty if valid)
   * @throws HopException if schema fails to compile
   */
  public static List<String> validateXml(String xmlContent, String xsdContent) throws HopException {
    Schema schema = compileSchema(xsdContent);
    return validateXml(xmlContent, schema);
  }

  /**
   * Validates a FileObject against a compiled Schema, returning any errors.
   *
   * @param file VFS FileObject
   * @param schema compiled Schema
   * @return list of validation error messages (empty if valid)
   * @throws HopException if file reading fails
   */
  public static List<String> validateFile(FileObject file, Schema schema) throws HopException {
    try (InputStream in = HopVfs.getInputStream(file)) {
      String xml = IOUtils.toString(in, StandardCharsets.UTF_8);
      return validateXml(xml, schema);
    } catch (Exception e) {
      throw new HopException("Error reading XML file " + file.getName().getURI(), e);
    }
  }

  /**
   * Validates a FileObject against an XSD schema string, returning any errors.
   *
   * @param file VFS FileObject
   * @param xsdContent XSD schema string
   * @return list of validation error messages (empty if valid)
   * @throws HopException if schema compilation or file reading fails
   */
  public static List<String> validateFile(FileObject file, String xsdContent) throws HopException {
    Schema schema = compileSchema(xsdContent);
    return validateFile(file, schema);
  }

  /** Validates XML content against an XSD schema string, throwing HopException if invalid. */
  public static void validate(String xmlContent, String xsdContent) throws HopException {
    List<String> errors = validateXml(xmlContent, xsdContent);
    if (!errors.isEmpty()) {
      throw new HopException(
          "XML Schema validation failed with " + errors.size() + " error(s): " + errors.get(0));
    }
  }

  /** Validates an XML file against an XSD schema string, throwing HopException if invalid. */
  public static void validate(FileObject xmlFile, String xsdContent, IVariables variables)
      throws HopException {
    List<String> errors = validateFile(xmlFile, xsdContent);
    if (!errors.isEmpty()) {
      throw new HopException(
          "XML Schema validation failed with " + errors.size() + " error(s): " + errors.get(0));
    }
  }

  /** Validates a pipeline file against the generated Pipeline XML Schema. */
  public static void validatePipeline(FileObject pipelineFile, IVariables variables)
      throws HopException {
    String xsd =
        HopXmlSchemaService.getInstance().generatePipelineSchema(new HopXmlSchemaExportOptions());
    validate(pipelineFile, xsd, variables);
  }

  /** Validates a workflow file against the generated Workflow XML Schema. */
  public static void validateWorkflow(FileObject workflowFile, IVariables variables)
      throws HopException {
    String xsd =
        HopXmlSchemaService.getInstance().generateWorkflowSchema(new HopXmlSchemaExportOptions());
    validate(workflowFile, xsd, variables);
  }
}
