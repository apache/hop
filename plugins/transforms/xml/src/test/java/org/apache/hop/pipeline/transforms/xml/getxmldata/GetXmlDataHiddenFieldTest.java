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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
 * Regression: Hidden additional field must use {@code data.hidden} from {@code
 * FileObject.isHidden()}, not {@code Boolean.valueOf(data.path)}.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataHiddenFieldTest {

  @TempDir Path tempDir;

  @BeforeEach
  void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testHiddenFileFieldUsesFileIsHiddenFlag() throws Exception {
    Path xmlFile = tempDir.resolve("hidden-sample.xml");
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root><row><name>x</name></row></root>\n";
    Files.writeString(xmlFile, xml, StandardCharsets.UTF_8);

    try {
      Files.setAttribute(xmlFile, "dos:hidden", true);
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      Path posixHidden = tempDir.resolve(".hidden-sample.xml");
      Files.move(xmlFile, posixHidden, StandardCopyOption.REPLACE_EXISTING);
      xmlFile = posixHidden;
    }

    assumeTrue(
        Files.isHidden(xmlFile),
        "OS/VFS could not mark test file as hidden; skip on this platform");

    String fileUri = xmlFile.toAbsolutePath().toUri().toString();
    // Parent path is never the literal "true", so Boolean.valueOf(path) would be false.
    assertFalse(Boolean.valueOf(xmlFile.getParent().toString()));

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("getxmldata-hidden-field");

    PluginRegistry registry = PluginRegistry.getInstance();

    GetXmlDataMeta gxdm = new GetXmlDataMeta();
    gxdm.setInFields(false);
    gxdm.setAFile(false);
    gxdm.setReadUrl(false);
    gxdm.setEncoding(StandardCharsets.UTF_8.name());
    gxdm.setLoopXPath("root/row");
    gxdm.setDoNotFailIfNoFile(false);
    gxdm.setPathFieldName("dir_path");
    gxdm.setHiddenFieldName("is_hidden");
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
    assertEquals("x", rows.get(0).getString("name_out", ""));
    int hiddenIdx = rows.get(0).getRowMeta().indexOfValue("is_hidden");
    assertTrue(hiddenIdx >= 0);
    Object hiddenValue = rows.get(0).getData()[hiddenIdx];
    assertInstanceOf(Boolean.class, hiddenValue);
    assertTrue((Boolean) hiddenValue, "is_hidden must reflect FileObject.isHidden(), not path");
  }
}
