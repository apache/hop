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

package org.apache.hop.pipeline.transforms.janino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JaninoMetaTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(), ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(), ValueMetaNumber.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  // ------------------------------------------------------------------ load/save

  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/janino.xml")).toURI());
    String xml = Files.readString(path);
    JaninoMeta meta = new JaninoMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        JaninoMeta.class,
        meta,
        new MemoryMetadataProvider());

    validateFixture(meta);

    // Do a round trip:
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    JaninoMeta metaCopy = new JaninoMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xmlCopy, TransformMeta.XML_TAG),
        JaninoMeta.class,
        metaCopy,
        new MemoryMetadataProvider());
    validateFixture(metaCopy);
  }

  private static void validateFixture(JaninoMeta meta) {
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
    assertEquals(3, meta.getFunctions().size());
    JaninoMetaFunction f = meta.getFunctions().getFirst();
    assertEquals("f1", f.getFieldName());
    assertEquals("expression1", f.getFormula());
    assertEquals(IValueMeta.TYPE_STRING, f.getValueType());
    assertEquals(100, f.getValueLength());
    assertEquals(-1, f.getValuePrecision());
    assertEquals("replace1", f.getReplaceField());

    f = meta.getFunctions().get(1);
    assertEquals("f2", f.getFieldName());
    assertEquals("expression2", f.getFormula());
    assertEquals(IValueMeta.TYPE_INTEGER, f.getValueType());
    assertEquals(7, f.getValueLength());
    assertEquals(-1, f.getValuePrecision());
    assertEquals("replace2", f.getReplaceField());

    f = meta.getFunctions().get(2);
    assertEquals("f3", f.getFieldName());
    assertEquals("expression3", f.getFormula());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getValueType());
    assertEquals(9, f.getValueLength());
    assertEquals(2, f.getValuePrecision());
    assertEquals("replace3", f.getReplaceField());
  }

  // ------------------------------------------------------------------
  // getEffectiveJavaTargetVersion

  @Test
  void effectiveVersion_default_returnsDefault() {
    JaninoMeta meta = new JaninoMeta();
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
  }

  @Test
  void effectiveVersion_valid_returnsAsIs() {
    JaninoMeta meta = new JaninoMeta();
    meta.setJavaTargetVersion(11);
    assertEquals(11, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(JaninoMeta.JAVA_TARGET_VERSION_MIN);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_MIN, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(JaninoMeta.JAVA_TARGET_VERSION_MAX);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_MAX, meta.getEffectiveJavaTargetVersion());
  }

  @Test
  void effectiveVersion_belowMin_returnsDefault() {
    JaninoMeta meta = new JaninoMeta();
    meta.setJavaTargetVersion(0);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(-1);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
  }

  @Test
  void effectiveVersion_aboveMax_returnsDefault() {
    JaninoMeta meta = new JaninoMeta();
    meta.setJavaTargetVersion(99);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
  }

  // ------------------------------------------------------------------ clone

  @Test
  void clone_copiesVersion() {
    JaninoMeta meta = new JaninoMeta();
    meta.setJavaTargetVersion(17);
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName("x");
    fn.setFormula("1");
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    meta.getFunctions().add(fn);

    JaninoMeta copy = (JaninoMeta) meta.clone();

    assertEquals(17, copy.getJavaTargetVersion());
    assertEquals(1, copy.getFunctions().size());
    assertNotSame(meta.getFunctions().get(0), copy.getFunctions().get(0));
  }

  // ------------------------------------------------------------------ getFields

  @Test
  void getFields_newField_addsToRow() throws HopTransformException {
    JaninoMeta meta = new JaninoMeta();
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName("added");
    fn.setFormula("1+1");
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    fn.setValueLength(9);
    fn.setValuePrecision(0);
    meta.getFunctions().add(fn);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("existing"));

    meta.getFields(row, "step", null, null, null, mock(IHopMetadataProvider.class));

    assertEquals(2, row.size());
    assertEquals("added", row.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(1).getType());
  }

  @Test
  void getFields_replaceField_changesExistingEntry() throws HopTransformException {
    JaninoMeta meta = new JaninoMeta();
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName("n");
    fn.setFormula("n*2");
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    fn.setValueLength(9);
    fn.setValuePrecision(0);
    fn.setReplaceField("n");
    meta.getFunctions().add(fn);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("n"));

    meta.getFields(row, "step", null, null, null, mock(IHopMetadataProvider.class));

    assertEquals(1, row.size()); // replaced, not appended
  }

  @Test
  void getFields_replaceFieldMissing_throwsHopTransformException() {
    JaninoMeta meta = new JaninoMeta();
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName("x");
    fn.setFormula("1");
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    fn.setReplaceField("nonexistent");
    meta.getFunctions().add(fn);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("other"));

    assertThrows(
        HopTransformException.class,
        () -> meta.getFields(row, "step", null, null, null, mock(IHopMetadataProvider.class)));
  }

  @Test
  void getFields_emptyFieldName_skipped() throws HopTransformException {
    JaninoMeta meta = new JaninoMeta();
    JaninoMetaFunction fn = new JaninoMetaFunction();
    fn.setFieldName("");
    fn.setFormula("1");
    fn.setValueType(IValueMeta.TYPE_INTEGER);
    meta.getFunctions().add(fn);

    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("existing"));

    meta.getFields(row, "step", null, null, null, mock(IHopMetadataProvider.class));
    assertEquals(1, row.size()); // nothing added
  }

  // ------------------------------------------------------------------ check

  @Test
  void check_noPrevFields_addsWarning() {
    JaninoMeta meta = new JaninoMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        null,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING));
  }

  @Test
  void check_withPrevFields_addsOk() {
    JaninoMeta meta = new JaninoMeta();
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("field1"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }

  @Test
  void check_noInputTransforms_addsError() {
    JaninoMeta meta = new JaninoMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_withInputTransforms_addsOk() {
    JaninoMeta meta = new JaninoMeta();
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("f"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }

  // ------------------------------------------------------------------ supportsErrorHandling

  @Test
  void supportsErrorHandling_returnsTrue() {
    assertTrue(new JaninoMeta().supportsErrorHandling());
  }
}
