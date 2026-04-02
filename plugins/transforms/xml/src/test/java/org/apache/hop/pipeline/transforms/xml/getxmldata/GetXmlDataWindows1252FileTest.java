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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.xml.RowTransformCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ensures file-based parsing honors the transform encoding when the XML declaration omits
 * encoding="..." (SAX must use {@link org.dom4j.io.SAXReader#setEncoding}, not read(InputStream,
 * String) which treats the second argument as systemId).
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataWindows1252FileTest {

  @TempDir Path tempDir;

  @BeforeEach
  void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testWindows1252FileWithoutEncodingInDeclaration() throws Exception {
    assumeTrue(Charset.isSupported("Windows-1252"));

    Charset cp1252 = Charset.forName("Windows-1252");
    // Prolog without encoding: parser must use transform encoding (Windows-1252).
    // U+00E9 is single byte 0xE9 in cp1252; UTF-8 would be 0xC3 0xA9 and breaks if misread.
    String logicalXml = "<?xml version=\"1.0\"?>\n<root><row><name>caf\u00e9</name></row></root>\n";
    Path xmlFile = tempDir.resolve("cp1252-sample.xml");
    Files.write(xmlFile, logicalXml.getBytes(cp1252));

    String fileUri = xmlFile.toAbsolutePath().toUri().toString();

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("getxmldata-cp1252-file");

    PluginRegistry registry = PluginRegistry.getInstance();

    GetXmlDataMeta gxdm = new GetXmlDataMeta();
    gxdm.setInFields(false);
    gxdm.setAFile(false);
    gxdm.setReadUrl(false);
    gxdm.setEncoding("Windows-1252");
    gxdm.setLoopXPath("root/row");
    gxdm.setDoNotFailIfNoFile(false);
    gxdm.setFilesList(
        List.of(new GetXmlFileItem(fileUri, "", "", GetXmlDataMeta.RequiredFilesCode[1], "N")));

    GetXmlDataField nameField = new GetXmlDataField("name_out");
    nameField.setXPath("name");
    nameField.setElementType(GetXmlDataField.getElementTypeCode(GetXmlDataField.ELEMENT_TYPE_NODE));
    nameField.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    nameField.setFormat("");
    nameField.setLength(-1);
    nameField.setPrecision(-1);
    nameField.setCurrencySymbol("");
    nameField.setDecimalSymbol("");
    nameField.setGroupSymbol("");
    nameField.setTrimType(GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_NONE));
    gxdm.setInputFields(List.of(nameField));

    String getXmlPid = registry.getPluginId(TransformPluginType.class, gxdm);
    TransformMeta getXmlTransform = new TransformMeta(getXmlPid, "get xml data", gxdm);
    pipelineMeta.addTransform(getXmlTransform);

    DummyMeta dm = new DummyMeta();
    String dummyPid = registry.getPluginId(TransformPluginType.class, dm);
    TransformMeta dummyTransform = new TransformMeta(dummyPid, "dummy", dm);
    pipelineMeta.addTransform(dummyTransform);

    pipelineMeta.addPipelineHop(new PipelineHopMeta(getXmlTransform, dummyTransform));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();

    ITransform dummyStep = pipeline.getTransform("dummy", 0);
    RowTransformCollector collector = new RowTransformCollector();
    dummyStep.addRowListener(collector);

    pipeline.startThreads();
    pipeline.waitUntilFinished();

    List<RowMetaAndData> rows = collector.getRowsWritten();
    assertEquals(1, rows.size(), "Expected one row from one <row> element");
    assertEquals("caf\u00e9", rows.get(0).getString("name_out", ""));
  }
}
