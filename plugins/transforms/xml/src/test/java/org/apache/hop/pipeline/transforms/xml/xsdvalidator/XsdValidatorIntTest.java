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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.xml.PipelineTestFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XsdValidatorIntTest {

  private static final String RAM_DIR = "ram://" + XsdValidatorIntTest.class.getSimpleName();
  private static final String TEST_FILES_DIR = "src/test/resources/xsdvalidator/";

  private static FileObject schemaRamFile = null;
  private static FileObject dataRamFile = null;

  @BeforeEach
  void init() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @AfterAll
  static void tearDownAfterClass() {
    try {
      if (schemaRamFile != null && schemaRamFile.exists()) {
        schemaRamFile.delete();
      }
      if (dataRamFile != null && dataRamFile.exists()) {
        dataRamFile.delete();
      }
    } catch (Exception ignore) {
      // Ignore
    }
  }

  @Test
  void testVfsInputFiles() throws Exception {
    testVfsFileTypes(getDataRamFile().getURL().toString(), getSchemaRamFile().getURL().toString());
    testVfsFileTypes(
        getDataRamFile().getURL().toString(), getSchemaFileUrl(TEST_FILES_DIR + "schema.xsd"));
    testVfsFileTypes(
        getDataFileUrl(TEST_FILES_DIR + "data.xml"), getSchemaRamFile().getURL().toString());
    testVfsFileTypes(
        getDataFileUrl(TEST_FILES_DIR + "data.xml"),
        getSchemaFileUrl(TEST_FILES_DIR + "schema.xsd"));
    testVfsFileTypes(
        getDataFileUrl(TEST_FILES_DIR + "xsd_issue/bad.xml"),
        getSchemaFileUrl(TEST_FILES_DIR + "xsd_issue/cbc-xml-schema-v1.0/CbcXML_v1.0.xsd"));
  }

  private FileObject getSchemaRamFile() throws Exception {
    if (schemaRamFile != null
        && schemaRamFile.exists()
        && schemaRamFile.getContent().getSize() > 0) {
      return schemaRamFile;
    }
    schemaRamFile = loadRamFile("schema.xsd");
    return schemaRamFile;
  }

  private FileObject getDataRamFile() throws Exception {
    if (dataRamFile != null && dataRamFile.exists() && dataRamFile.getContent().getSize() > 0) {
      return dataRamFile;
    }
    dataRamFile = loadRamFile("data.xml");
    return dataRamFile;
  }

  private String getFileUrl(String filename) throws Exception {
    File file = new File(filename);
    return file.toURI().toURL().toExternalForm();
  }

  private InputStream getFileInputStream(String filename) throws Exception {
    File file = new File(TEST_FILES_DIR + filename);
    return new FileInputStream(file);
  }

  private String getSchemaFileUrl(String filename) throws Exception {
    return getFileUrl(filename);
  }

  private String getDataFileUrl(String filename) throws Exception {
    return getFileUrl(filename);
  }

  private FileObject loadRamFile(String filename) throws Exception {
    String targetUrl = RAM_DIR + "/" + filename;
    try (InputStream source = getFileInputStream(filename)) {
      FileObject fileObject = HopVfs.getFileObject(targetUrl, new Variables());
      try (OutputStream targetStream = fileObject.getContent().getOutputStream()) {
        copy(source, targetStream);
      }
      return fileObject;
    }
  }

  private void testVfsFileTypes(String dataFilename, String schemaFilename) throws Exception {
    assertNotNull(dataFilename);
    assertNotNull(schemaFilename);
    assertTrue(HopVfs.getFileObject(dataFilename, new Variables()).exists());
    assertTrue(HopVfs.getFileObject(schemaFilename, new Variables()).exists());

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("DataFile"));
    inputRowMeta.addValueMeta(new ValueMetaString("SchemaFile"));
    List<RowMetaAndData> inputData = new ArrayList<>();
    inputData.add(new RowMetaAndData(inputRowMeta, new Object[] {dataFilename, schemaFilename}));

    String transformName = "XSD Validator";
    XsdValidatorMeta meta = new XsdValidatorMeta();
    meta.setDefault();
    meta.setXmlSourceFile(true);
    meta.setXmlStream("DataFile");
    meta.setXsdSource(XsdValidatorMeta.SPECIFY_FIELDNAME);
    meta.setXsdDefinedField("SchemaFile");
    meta.setAddValidationMessage(true);
    PipelineMeta pipelineMeta =
        PipelineTestFactory.generateTestTransformation(null, meta, transformName);

    List<RowMetaAndData> result = null;
    result =
        PipelineTestFactory.executeTestTransformation(
            pipelineMeta,
            PipelineTestFactory.INJECTOR_TRANSFORMNAME,
            transformName,
            PipelineTestFactory.DUMMY_TRANSFORMNAME,
            inputData);

    assertNotNull(result);
    assertEquals(1, result.size());

    // Check Metadata
    assertEquals(IValueMeta.TYPE_STRING, result.get(0).getValueMeta(0).getType());
    assertEquals(IValueMeta.TYPE_STRING, result.get(0).getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_BOOLEAN, result.get(0).getValueMeta(2).getType());
    assertEquals("DataFile", result.get(0).getValueMeta(0).getName());
    assertEquals("SchemaFile", result.get(0).getValueMeta(1).getName());
    assertEquals("result", result.get(0).getValueMeta(2).getName());

    // Check result
    assertEquals(dataFilename, result.get(0).getString(0, "default"));
    assertEquals(schemaFilename, result.get(0).getString(1, "default"));
    assertTrue(result.get(0).getBoolean(2, false));
  }

  private static void copy(InputStream inp, OutputStream out) throws IOException {
    final byte[] buff = new byte[4096];
    int readBytes = -1;
    do {
      int todoBytes = buff.length;
      readBytes = inp.read(buff, 0, todoBytes);
      if (readBytes > 0) {
        out.write(buff, 0, readBytes);
      }
    } while (readBytes >= 0);
  }
}
