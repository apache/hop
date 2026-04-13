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
package org.apache.hop.pipeline.transforms.streamlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit tests for {@link StreamLookupMeta}. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class StreamLookupMetaTest {

  @BeforeEach
  void beforeEach() throws Exception {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    StreamLookupMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/stream-lookup.xml", StreamLookupMeta.class);

    assertTrue(meta.isMemoryPreservationActive());
    assertTrue(meta.isUsingIntegerPair());
    assertTrue(meta.isUsingSortedList());

    assertEquals(2, meta.getLookup().getMatchKeys().size());
    StreamLookupMeta.MatchKey k = meta.getLookup().getMatchKeys().getFirst();
    assertEquals("keyStream1", k.getKeyStream());
    assertEquals("keyLookup1", k.getKeyLookup());

    k = meta.getLookup().getMatchKeys().getLast();
    assertEquals("keyStream2", k.getKeyStream());
    assertEquals("keyLookup2", k.getKeyLookup());

    assertEquals(3, meta.getLookup().getReturnValues().size());
    StreamLookupMeta.ReturnValue v = meta.getLookup().getReturnValues().getFirst();
    assertEquals("returnValue1", v.getValue());
    assertEquals("value1", v.getValueName());
    assertTrue(StringUtils.isEmpty(v.getValueDefault()));
    assertEquals(IValueMeta.TYPE_STRING, v.getValueDefaultType());

    v = meta.getLookup().getReturnValues().get(1);
    assertEquals("returnValue2", v.getValue());
    assertEquals("value2", v.getValueName());
    assertEquals("123", v.getValueDefault());
    assertEquals(IValueMeta.TYPE_INTEGER, v.getValueDefaultType());

    v = meta.getLookup().getReturnValues().getLast();
    assertEquals("returnValue3", v.getValue());
    assertEquals("value3", v.getValueName());
    assertEquals("true", v.getValueDefault());
    assertEquals(IValueMeta.TYPE_BOOLEAN, v.getValueDefaultType());
  }

  @Test
  void constructorInitializesDefaults() {
    StreamLookupMeta meta = new StreamLookupMeta();
    assertTrue(meta.isMemoryPreservationActive());
    assertNotNull(meta.getLookup());
    assertTrue(meta.getLookup().getMatchKeys().isEmpty());
    assertTrue(meta.getLookup().getReturnValues().isEmpty());
  }

  @Test
  void copyConstructorDeepCopiesLookup() {
    StreamLookupMeta original = new StreamLookupMeta();
    original.setSourceTransformName("A");
    original.setInputSorted(true);
    original.setUsingSortedList(true);
    original.setUsingIntegerPair(true);
    original.setMemoryPreservationActive(false);

    StreamLookupMeta.MatchKey mk = new StreamLookupMeta.MatchKey();
    mk.setKeyStream("k1");
    mk.setKeyLookup("lk1");
    original.getLookup().getMatchKeys().add(mk);

    StreamLookupMeta.ReturnValue rv = new StreamLookupMeta.ReturnValue();
    rv.setValue("v");
    rv.setValueName("vn");
    rv.setValueDefault("d");
    rv.setValueDefaultType(IValueMeta.TYPE_INTEGER);
    original.getLookup().getReturnValues().add(rv);

    StreamLookupMeta copy = new StreamLookupMeta(original);
    assertEquals(original.getSourceTransformName(), copy.getSourceTransformName());
    assertEquals(original.isInputSorted(), copy.isInputSorted());
    assertEquals(original.isUsingSortedList(), copy.isUsingSortedList());
    assertEquals(original.isUsingIntegerPair(), copy.isUsingIntegerPair());
    assertEquals(original.isMemoryPreservationActive(), copy.isMemoryPreservationActive());

    assertNotSame(original.getLookup(), copy.getLookup());
    assertEquals(1, copy.getLookup().getMatchKeys().size());
    assertEquals("k1", copy.getLookup().getMatchKeys().getFirst().getKeyStream());
    assertEquals(1, copy.getLookup().getReturnValues().size());
    assertEquals("v", copy.getLookup().getReturnValues().getFirst().getValue());
  }

  @Test
  void cloneMatchesCopyConstructorSemantics() {
    StreamLookupMeta meta = new StreamLookupMeta();
    meta.setSourceTransformName("Src");
    StreamLookupMeta.MatchKey mk = new StreamLookupMeta.MatchKey();
    mk.setKeyStream("a");
    mk.setKeyLookup("b");
    meta.getLookup().getMatchKeys().add(mk);

    StreamLookupMeta cloned = (StreamLookupMeta) meta.clone();
    assertNotSame(meta, cloned);
    assertEquals(meta.getSourceTransformName(), cloned.getSourceTransformName());
    assertNotSame(meta.getLookup(), cloned.getLookup());
    assertEquals(
        meta.getLookup().getMatchKeys().getFirst().getKeyStream(),
        cloned.getLookup().getMatchKeys().getFirst().getKeyStream());
  }

  @Test
  void excludeFromRowLayoutVerificationIsTrue() {
    assertTrue(new StreamLookupMeta().excludeFromRowLayoutVerification());
  }

  @Test
  void getTransformIOMetaReturnsSameInstanceAndUsesSourceTransformNameAsInitialSubject() {
    StreamLookupMeta meta = new StreamLookupMeta();
    meta.setSourceTransformName("Lookup source");
    ITransformIOMeta io1 = meta.getTransformIOMeta();
    ITransformIOMeta io2 = meta.getTransformIOMeta();
    assertSame(io1, io2);
    assertEquals(1, io1.getInfoStreams().size());
    assertEquals("Lookup source", io1.getInfoStreams().getFirst().getSubject());
  }

  @Test
  void resetTransformIoMetaDoesNotClearCachedIoMeta() {
    StreamLookupMeta meta = new StreamLookupMeta();
    ITransformIOMeta io = meta.getTransformIOMeta();
    meta.resetTransformIoMeta();
    assertSame(io, meta.getTransformIOMeta());
  }

  /** Unit test for {@link StreamLookupMeta#getFields}. */
  @Nested
  class GetFieldsTest {

    @Test
    void withSingleInfoRowCopiesFieldsFromLookupStreamAndRenames() throws HopTransformException {
      StreamLookupMeta meta = new StreamLookupMeta();
      StreamLookupMeta.ReturnValue rv = new StreamLookupMeta.ReturnValue();
      rv.setValue("name");
      rv.setValueName("out_name");
      rv.setValueDefaultType(IValueMeta.TYPE_STRING);
      meta.getLookup().getReturnValues().add(rv);

      IRowMeta lookupRow = new RowMetaBuilder().addString("name").build();
      RowMeta out = new RowMeta();
      meta.getFields(out, "origin", new IRowMeta[] {lookupRow}, null, new Variables(), null);

      assertEquals(1, out.size());
      assertEquals("out_name", out.getValueMeta(0).getName());
      assertEquals("origin", out.getValueMeta(0).getOrigin());
    }

    @Test
    void withSingleInfoRowThrowsWhenReturnFieldMissingFromLookup() {
      StreamLookupMeta meta = new StreamLookupMeta();
      StreamLookupMeta.ReturnValue rv = new StreamLookupMeta.ReturnValue();
      rv.setValue("missing");
      rv.setValueName("x");
      rv.setValueDefaultType(IValueMeta.TYPE_STRING);
      meta.getLookup().getReturnValues().add(rv);

      IRowMeta lookupRow = new RowMetaBuilder().addString("name").build();
      RowMeta out = new RowMeta();
      assertThrows(
          HopTransformException.class,
          () -> meta.getFields(out, "o", new IRowMeta[] {lookupRow}, null, new Variables(), null));
    }

    @Test
    void withoutValidInfoCreatesValueMetaFromDefaults() throws HopTransformException {
      StreamLookupMeta meta = new StreamLookupMeta();
      StreamLookupMeta.ReturnValue rv = new StreamLookupMeta.ReturnValue();
      rv.setValueName("col");
      rv.setValueDefaultType(IValueMeta.TYPE_INTEGER);
      meta.getLookup().getReturnValues().add(rv);

      RowMeta out = new RowMeta();
      meta.getFields(out, "origin", null, null, new Variables(), null);

      assertEquals(1, out.size());
      assertEquals("col", out.getValueMeta(0).getName());
      assertEquals(IValueMeta.TYPE_INTEGER, out.getValueMeta(0).getType());
      assertEquals("origin", out.getValueMeta(0).getOrigin());
    }

    @Test
    void withInfoArrayWrongShapeUsesDefaultValueMetaPath() throws HopTransformException {
      StreamLookupMeta meta = new StreamLookupMeta();
      StreamLookupMeta.ReturnValue rv = new StreamLookupMeta.ReturnValue();
      rv.setValueName("only");
      rv.setValueDefaultType(IValueMeta.TYPE_STRING);
      meta.getLookup().getReturnValues().add(rv);

      RowMeta out = new RowMeta();
      IRowMeta[] twoInfos = {
        new RowMetaBuilder().addString("a").build(), new RowMetaBuilder().addString("b").build()
      };
      meta.getFields(out, "origin", twoInfos, null, new Variables(), null);

      assertEquals(1, out.size());
      assertEquals("only", out.getValueMeta(0).getName());
    }
  }

  @Nested
  class CheckTest {

    private StreamLookupMeta metaWithMinimalValidConfig() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("Lookup");
      meta.searchInfoAndTargetTransforms(List.of(namedTransform("Lookup")));

      StreamLookupMeta.MatchKey mk = new StreamLookupMeta.MatchKey();
      mk.setKeyStream("id");
      mk.setKeyLookup("lid");
      meta.getLookup().getMatchKeys().add(mk);

      StreamLookupMeta.ReturnValue rv = new StreamLookupMeta.ReturnValue();
      rv.setValue("ret");
      rv.setValueName("ret_out");
      rv.setValueDefaultType(IValueMeta.TYPE_STRING);
      meta.getLookup().getReturnValues().add(rv);

      return meta;
    }

    @Test
    void checkAddsErrorWhenPreviousRowIsEmpty() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMeta();
      IRowMeta info = new RowMetaBuilder().addInteger("lid").addString("ret").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }

    @Test
    void checkAddsErrorWhenMatchKeyMissingFromPrevious() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta info = new RowMetaBuilder().addInteger("id").addString("name").build();
      IRowMeta prev = new RowMetaBuilder().addInteger("wrong").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }

    @Test
    void checkAddsErrorWhenFewerThanTwoInputHops() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMetaBuilder().addInteger("id").build();
      IRowMeta info = new RowMetaBuilder().addInteger("lid").addString("ret").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"only"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }

    @Test
    void checkAddsErrorWhenKeyLookupFieldMissingFromInfoRow() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMetaBuilder().addInteger("id").build();
      IRowMeta info = new RowMetaBuilder().addInteger("wrong_lid").addString("ret").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }

    @Test
    void checkAddsErrorWhenReturnValueFieldMissingFromInfoRow() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMetaBuilder().addInteger("id").build();
      IRowMeta info = new RowMetaBuilder().addInteger("lid").addString("other").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }

    @Test
    void checkNoErrorsWhenConfigurationAndRowsAreConsistent() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMetaBuilder().addInteger("id").build();
      IRowMeta info = new RowMetaBuilder().addInteger("lid").addString("ret").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertEquals(0, countErrors(remarks));
    }

    @Test
    void checkSourceTransformErrorWhenInfoStreamHasNoTransformMeta() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      meta.getTransformIOMeta().getInfoStreams().getFirst().setTransformMeta(null);

      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMetaBuilder().addInteger("id").build();
      IRowMeta info = new RowMetaBuilder().addInteger("lid").addString("ret").build();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }

    @Test
    void checkAddsErrorWhenLookupInfoRowIsEmpty() {
      StreamLookupMeta meta = metaWithMinimalValidConfig();
      List<ICheckResult> remarks = new ArrayList<>();
      IRowMeta prev = new RowMetaBuilder().addInteger("id").build();
      IRowMeta info = new RowMeta();

      meta.check(
          remarks,
          null,
          new TransformMeta("SL", meta),
          prev,
          new String[] {"a", "b"},
          new String[] {"o"},
          info,
          new Variables(),
          null);

      assertTrue(countErrors(remarks) >= 1);
    }
  }

  /** Unit test for {@link StreamLookupMeta#handleStreamSelection}. */
  @Nested
  class HandleStreamSelectionTest {

    @Test
    void updatesSourceTransformNameWhenInfoStreamIsRepointed() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("OldLookup");
      IStream infoStream = meta.getTransformIOMeta().getInfoStreams().getFirst();
      TransformMeta inserted = namedTransform("InsertedStep");
      infoStream.setTransformMeta(inserted);
      infoStream.setSubject("InsertedStep");

      meta.handleStreamSelection(infoStream);

      assertEquals("InsertedStep", meta.getSourceTransformName());
      assertEquals("InsertedStep", infoStream.getSubject());
    }

    @Test
    void searchInfoAfterHandleStreamSelectionKeepsInsertedLookupSource() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("OldLookup");
      IStream infoStream = meta.getTransformIOMeta().getInfoStreams().getFirst();
      TransformMeta inserted = namedTransform("InsertedStep");
      infoStream.setTransformMeta(inserted);
      infoStream.setSubject("InsertedStep");
      meta.handleStreamSelection(infoStream);

      meta.searchInfoAndTargetTransforms(List.of(inserted, namedTransform("Other")));

      assertSame(inserted, infoStream.getTransformMeta());
      assertEquals("InsertedStep", meta.getSourceTransformName());
    }

    @Test
    void ignoresStreamsThatAreNotInfoStreams() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("KeepMe");
      IStream other = mock(IStream.class);
      meta.handleStreamSelection(other);
      assertEquals("KeepMe", meta.getSourceTransformName());
    }
  }

  /** Unit test for {@link StreamLookupMeta#searchInfoAndTargetTransforms}. */
  @Nested
  class SearchInfoAndTargetTransformsTest {

    @Test
    void resolvesFromSourceTransformNameAndSyncsInfoStreamSubjectWhenStreamSubjectWasStale() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("Data grid");
      IStream infoStream = meta.getTransformIOMeta().getInfoStreams().getFirst();
      infoStream.setSubject("wrong-or-empty-subject");

      TransformMeta dataGrid = namedTransform("Data grid");
      meta.searchInfoAndTargetTransforms(List.of(dataGrid));

      assertSame(dataGrid, infoStream.getTransformMeta());
      assertEquals("Data grid", infoStream.getSubject());
    }

    @Test
    void whenSourceTransformNameBlankUsesExistingStreamSubject() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName(null);
      IStream infoStream = meta.getTransformIOMeta().getInfoStreams().getFirst();
      infoStream.setSubject("Data grid");

      TransformMeta dataGrid = namedTransform("Data grid");
      meta.searchInfoAndTargetTransforms(List.of(dataGrid));

      assertSame(dataGrid, infoStream.getTransformMeta());
    }

    @Test
    void whenSourceTransformNameIsWhitespaceOnlyFallsBackToStreamSubject() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("   ");
      IStream infoStream = meta.getTransformIOMeta().getInfoStreams().getFirst();
      infoStream.setSubject("Data grid");

      TransformMeta dataGrid = namedTransform("Data grid");
      meta.searchInfoAndTargetTransforms(List.of(dataGrid));

      assertSame(dataGrid, infoStream.getTransformMeta());
    }

    @Test
    void trimsLookupNameWhenResolvingAgainstPipelineTransforms() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("  Data grid  ");
      meta.searchInfoAndTargetTransforms(List.of(namedTransform("Data grid")));

      IStream infoStream = meta.getTransformIOMeta().getInfoStreams().getFirst();
      assertEquals("  Data grid  ", infoStream.getSubject());
      assertEquals("Data grid", infoStream.getTransformMeta().getName());
    }

    @Test
    void findTransformIsCaseInsensitive() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("data grid");
      TransformMeta dataGrid = namedTransform("Data grid");
      meta.searchInfoAndTargetTransforms(List.of(dataGrid));

      assertSame(
          dataGrid, meta.getTransformIOMeta().getInfoStreams().getFirst().getTransformMeta());
    }

    @Test
    void setsTransformMetaToNullWhenNoMatchingTransformExists() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("Unknown step");
      meta.searchInfoAndTargetTransforms(List.of(namedTransform("Add constants")));

      assertNull(meta.getTransformIOMeta().getInfoStreams().getFirst().getTransformMeta());
    }

    @Test
    void emptyTransformListYieldsNullTransformMeta() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("Data grid");
      meta.searchInfoAndTargetTransforms(List.of());

      assertNull(meta.getTransformIOMeta().getInfoStreams().getFirst().getTransformMeta());
    }

    @Test
    void nullTransformListIsHandled() {
      StreamLookupMeta meta = new StreamLookupMeta();
      meta.setSourceTransformName("Add constants");
      meta.searchInfoAndTargetTransforms(null);

      assertNull(meta.getTransformIOMeta().getInfoStreams().getFirst().getTransformMeta());
    }
  }

  private static long countErrors(List<ICheckResult> remarks) {
    return remarks.stream().filter(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR).count();
  }

  private static TransformMeta namedTransform(String name) {
    TransformMeta tm = mock(TransformMeta.class);
    when(tm.getName()).thenReturn(name);
    return tm;
  }
}
