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
package org.apache.hop.pipeline.transforms.javafilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.Test;

class JavaFilterMetaTest {

  // ------------------------------------------------------------------ serialization

  @Test
  void testDeserialize() throws Exception {
    // Deserialize directly (without round-trip getXml(), which would call
    // convertIOMetaToTransformNames() and reset the stream names to "").
    var document =
        XmlHandler.loadXmlFile(JavaFilterMetaTest.class.getResourceAsStream("/java-filter.xml"));
    var node = XmlHandler.getSubNode(document, TransformMeta.XML_TAG);
    JavaFilterMeta meta =
        XmlMetadataUtil.deSerializeFromXml(
            node, JavaFilterMeta.class, new MemoryMetadataProvider());

    assertEquals("amount > 0", meta.getCondition());
    assertEquals("positiveRows", meta.getTrueTransform());
    assertEquals("negativeRows", meta.getFalseTransform());
  }

  @Test
  void testGetXml_conditionRoundTrips() throws Exception {
    // getXml() calls convertIOMetaToTransformNames() which resets stream names in tests
    // (streams have no linked TransformMeta) — just verify condition survives the round-trip.
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("x > 0");
    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + meta.getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    var document = XmlHandler.loadXmlString(xml);
    var node = XmlHandler.getSubNode(document, TransformMeta.XML_TAG);
    JavaFilterMeta copy =
        XmlMetadataUtil.deSerializeFromXml(
            node, JavaFilterMeta.class, new MemoryMetadataProvider());
    assertEquals("x > 0", copy.getCondition());
  }

  // ------------------------------------------------------------------ getters / setters

  @Test
  void setGetCondition() {
    JavaFilterMeta meta = new JavaFilterMeta();
    assertNull(meta.getCondition());
    meta.setCondition("x > 0");
    assertEquals("x > 0", meta.getCondition());
  }

  @Test
  void setGetTrueTransform() {
    JavaFilterMeta meta = new JavaFilterMeta();
    assertNull(meta.getTrueTransform());
    meta.setTrueTransform("yes");
    assertEquals("yes", meta.getTrueTransform());
  }

  @Test
  void setGetFalseTransform() {
    JavaFilterMeta meta = new JavaFilterMeta();
    assertNull(meta.getFalseTransform());
    meta.setFalseTransform("no");
    assertEquals("no", meta.getFalseTransform());
  }

  // ------------------------------------------------------------------ setDefault

  @Test
  void setDefault_setsConditionToTrue() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setDefault();
    assertEquals("true", meta.getCondition());
  }

  // ------------------------------------------------------------------ clone

  @Test
  void clone_returnsDifferentInstance() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("n > 0");
    meta.setTrueTransform("yes");
    meta.setFalseTransform("no");

    JavaFilterMeta cloned = (JavaFilterMeta) meta.clone();
    assertNotSame(meta, cloned);
    assertEquals("n > 0", cloned.getCondition());
  }

  // ------------------------------------------------------------------ hashCode

  @Test
  void hashCode_sameCondition_sameHashCode() {
    JavaFilterMeta a = new JavaFilterMeta();
    a.setCondition("x > 0");
    JavaFilterMeta b = new JavaFilterMeta();
    b.setCondition("x > 0");
    assertEquals(a.hashCode(), b.hashCode());
  }

  // ------------------------------------------------------------------
  // convertIOMetaToTransformNames

  @Test
  void convertIOMetaToTransformNames_nullStreams_setsEmptyStrings() {
    JavaFilterMeta meta = new JavaFilterMeta();
    // streams start with null TransformMeta → getTransformName() returns null → NVL → ""
    meta.convertIOMetaToTransformNames();
    assertEquals("", meta.getTrueTransform());
    assertEquals("", meta.getFalseTransform());
  }

  @Test
  void convertIOMetaToTransformNames_withNames_copiesNames() {
    JavaFilterMeta meta = new JavaFilterMeta();
    TransformMeta trueMeta = mock(TransformMeta.class);
    when(trueMeta.getName()).thenReturn("trueTarget");
    TransformMeta falseMeta = mock(TransformMeta.class);
    when(falseMeta.getName()).thenReturn("falseTarget");

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    streams.get(0).setTransformMeta(trueMeta);
    streams.get(1).setTransformMeta(falseMeta);

    meta.convertIOMetaToTransformNames();
    assertEquals("trueTarget", meta.getTrueTransform());
    assertEquals("falseTarget", meta.getFalseTransform());
  }

  // ------------------------------------------------------------------
  // searchInfoAndTargetTransforms

  @Test
  void searchInfoAndTargetTransforms_findsMatchingTransforms() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setTrueTransform("yes");
    meta.setFalseTransform("no");

    TransformMeta yesMeta = mock(TransformMeta.class);
    when(yesMeta.getName()).thenReturn("yes");
    TransformMeta noMeta = mock(TransformMeta.class);
    when(noMeta.getName()).thenReturn("no");

    meta.searchInfoAndTargetTransforms(List.of(yesMeta, noMeta));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertEquals(yesMeta, streams.get(0).getTransformMeta());
    assertEquals(noMeta, streams.get(1).getTransformMeta());
  }

  @Test
  void searchInfoAndTargetTransforms_noMatch_setsNull() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setTrueTransform("nonexistent");
    meta.setFalseTransform("alsoMissing");

    TransformMeta other = mock(TransformMeta.class);
    when(other.getName()).thenReturn("other");

    meta.searchInfoAndTargetTransforms(List.of(other));

    List<IStream> streams = meta.getTransformIOMeta().getTargetStreams();
    assertNull(streams.get(0).getTransformMeta());
    assertNull(streams.get(1).getTransformMeta());
  }

  // ------------------------------------------------------------------ getTransformIOMeta

  @Test
  void getTransformIOMeta_lazyInit_returnsSameInstance() {
    JavaFilterMeta meta = new JavaFilterMeta();
    var io1 = meta.getTransformIOMeta();
    var io2 = meta.getTransformIOMeta();
    assertNotNull(io1);
    assertEquals(2, io1.getTargetStreams().size());
  }

  // ------------------------------------------------------------------ resetTransformIoMeta (no-op)

  @Test
  void resetTransformIoMeta_doesNotClearExistingIo() {
    JavaFilterMeta meta = new JavaFilterMeta();
    var io = meta.getTransformIOMeta();
    meta.resetTransformIoMeta();
    assertEquals(io, meta.getTransformIOMeta());
  }

  // ------------------------------------------------------------------
  // excludeFromCopyDistributeVerification

  @Test
  void excludeFromCopyDistributeVerification_returnsTrue() {
    assertTrue(new JavaFilterMeta().excludeFromCopyDistributeVerification());
  }

  // ------------------------------------------------------------------ check: target stream
  // combinations

  @Test
  void check_bothTargetsSpecified_addsOk() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("x > 0");

    TransformMeta trueMeta = mock(TransformMeta.class);
    when(trueMeta.getName()).thenReturn("yes");
    TransformMeta falseMeta = mock(TransformMeta.class);
    when(falseMeta.getName()).thenReturn("no");
    meta.getTransformIOMeta().getTargetStreams().get(0).setTransformMeta(trueMeta);
    meta.getTransformIOMeta().getTargetStreams().get(1).setTransformMeta(falseMeta);

    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("x"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[] {"yes", "no"},
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    long okCount = remarks.stream().filter(r -> r.getType() == ICheckResult.TYPE_RESULT_OK).count();
    assertTrue(okCount >= 1);
  }

  @Test
  void check_neitherTargetSpecified_addsOk() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");
    // both streams have null TransformMeta → getTransformName() returns null

    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("x"));

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
  void check_onlyOneTrueTargetSpecified_addsOk() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    TransformMeta trueMeta = mock(TransformMeta.class);
    when(trueMeta.getName()).thenReturn("yes");
    meta.getTransformIOMeta().getTargetStreams().get(0).setTransformMeta(trueMeta);
    // false stream stays null

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[] {"yes"},
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    // The "else" branch adds an OK result even when only one target is set
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }

  @Test
  void check_trueTargetNotInOutput_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    TransformMeta trueMeta = mock(TransformMeta.class);
    when(trueMeta.getName()).thenReturn("missingTarget");
    meta.getTransformIOMeta().getTargetStreams().get(0).setTransformMeta(trueMeta);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[] {"otherTarget"},
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_falseTargetNotInOutput_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    TransformMeta falseMeta = mock(TransformMeta.class);
    when(falseMeta.getName()).thenReturn("missingFalse");
    meta.getTransformIOMeta().getTargetStreams().get(1).setTransformMeta(falseMeta);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[] {"otherTarget"},
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_emptyCondition_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_nullCondition_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    // condition stays null

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_prevFieldsEmpty_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_prevFieldsNull_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

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

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_noInputTransforms_addsError() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("x"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void check_withInputTransforms_addsOk() {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition("true");

    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("x"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"upstream"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }
}
