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
package org.apache.hop.pipeline.transforms.fuzzymatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

/** Unit test for {@link FuzzyMatchMeta} */
class FuzzyMatchMetaTest {

  @Test
  void testSerialization() throws Exception {
    FuzzyMatchMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/fuzzy-match-transform.xml", FuzzyMatchMeta.class);

    assertEquals("Data grid", meta.getLookupTransformName());
    assertEquals("name", meta.getLookupField());
    assertEquals("name", meta.getMainStreamField());
    assertEquals("match", meta.getOutputMatchField());
    assertEquals("measure value", meta.getOutputValueField());
    assertFalse(meta.isCaseSensitive());
    assertTrue(meta.isCloserValue());
    assertEquals(FuzzyMatchMeta.MatchMode.CLOSEST, meta.getMatchMode());
    assertEquals("10", meta.getMaxMatches());
    assertEquals("0", meta.getMinimalValue());
    assertEquals("1", meta.getMaximalValue());
    assertEquals(",", meta.getSeparator());
    assertEquals(FuzzyMatchMeta.Algorithm.SOUNDEX, meta.getAlgorithm());
    assertEquals(1, meta.getLookupValues().size());
    assertEquals("name", meta.getLookupValues().get(0).getName());
    assertEquals("lookupName", meta.getLookupValues().get(0).getRename());
  }

  @Test
  void testSetDefault() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMaxMatches("3");
    meta.setCaseSensitive(true);

    meta.setDefault();

    assertEquals(FuzzyMatchMeta.Algorithm.NONE, meta.getAlgorithm());
    assertEquals(FuzzyMatchMeta.MatchMode.CLOSEST, meta.getMatchMode());
    assertTrue(meta.isCloserValue());
    assertEquals(String.valueOf(FuzzyMatchMeta.DEFAULT_MAX_MATCHES), meta.getMaxMatches());
    assertEquals(FuzzyMatchMeta.DEFAULT_SEPARATOR, meta.getSeparator());
    assertEquals("0", meta.getMinimalValue());
    assertEquals("1", meta.getMaximalValue());
    assertFalse(meta.isCaseSensitive());
    assertNotNull(meta.getOutputMatchField());
    assertNotNull(meta.getOutputValueField());
  }

  @Test
  void testCloneCopiesMatchModeAndLookupValues() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.JARO);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setMaxMatches("7");
    meta.setMainStreamField("main");
    meta.setLookupField("lookup");
    meta.setLookupValues(List.of(new FuzzyMatchMeta.FMLookupValue("id", "rid")));

    FuzzyMatchMeta copy = meta.clone();

    assertNotSame(meta, copy);
    assertEquals(FuzzyMatchMeta.MatchMode.ALL_ROWS, copy.getMatchMode());
    assertEquals("7", copy.getMaxMatches());
    assertEquals("main", copy.getMainStreamField());
    assertEquals(1, copy.getLookupValues().size());
    assertEquals("id", copy.getLookupValues().get(0).getName());
    assertEquals("rid", copy.getLookupValues().get(0).getRename());
    assertNotSame(meta.getLookupValues().get(0), copy.getLookupValues().get(0));
  }

  @Test
  void testLegacyCloserValueMapsToMatchMode() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setCloserValue(false);
    assertEquals(FuzzyMatchMeta.MatchMode.ALL_CONCAT, meta.getMatchMode());
    assertTrue(meta.isAllConcatMode());
    assertFalse(meta.isCloserValue());

    meta.setCloserValue(true);
    assertEquals(FuzzyMatchMeta.MatchMode.CLOSEST, meta.getMatchMode());
    assertTrue(meta.isCloserValue());
    assertFalse(meta.isAllRowsMode());
  }

  @Test
  void testSetCloserValueDoesNotOverwriteAllRows() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setCloserValue(true);

    assertEquals(FuzzyMatchMeta.MatchMode.ALL_ROWS, meta.getMatchMode());
    assertTrue(meta.isAllRowsMode());
    assertFalse(meta.isCloserValue());
  }

  @Test
  void testSetMatchModeNullFallsBackToClosest() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(null);
    assertEquals(FuzzyMatchMeta.MatchMode.CLOSEST, meta.getMatchMode());
    assertTrue(meta.isCloserValue());
  }

  @Test
  void testSupportsAdditionalFields() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);

    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    assertTrue(meta.supportsAdditionalFields());

    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    assertTrue(meta.supportsAdditionalFields());

    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_CONCAT);
    assertFalse(meta.supportsAdditionalFields());

    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.SOUNDEX);
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_CONCAT);
    assertTrue(meta.supportsAdditionalFields());
  }

  @Test
  void testGetFieldsAllRowsIncludesMeasureAndAdditionalFields() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setMaxMatches("10");
    meta.setLookupValues(List.of(new FuzzyMatchMeta.FMLookupValue("id", "lookup_id")));

    IRowMeta inputRowMeta = new RowMeta();
    IRowMeta lookupRowMeta = new RowMeta();
    lookupRowMeta.addValueMeta(new ValueMetaString("id"));

    meta.getFields(
        inputRowMeta, "FuzzyMatch", new IRowMeta[] {lookupRowMeta}, null, new Variables(), null);

    assertNotNull(inputRowMeta.searchValueMeta("match"));
    IValueMeta measure = inputRowMeta.searchValueMeta("measure value");
    assertNotNull(measure);
    assertEquals(IValueMeta.TYPE_INTEGER, measure.getType());
    assertNotNull(inputRowMeta.searchValueMeta("lookup_id"));
  }

  @Test
  void testGetFieldsConcatMeasureIsStringAndSkipsAdditionalFields() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_CONCAT);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setLookupValues(List.of(new FuzzyMatchMeta.FMLookupValue("id", "lookup_id")));

    IRowMeta inputRowMeta = new RowMeta();
    IRowMeta lookupRowMeta = new RowMeta();
    lookupRowMeta.addValueMeta(new ValueMetaString("id"));

    meta.getFields(
        inputRowMeta, "FuzzyMatch", new IRowMeta[] {lookupRowMeta}, null, new Variables(), null);

    IValueMeta measure = inputRowMeta.searchValueMeta("measure value");
    assertNotNull(measure);
    assertEquals(IValueMeta.TYPE_STRING, measure.getType());
    assertNull(inputRowMeta.searchValueMeta("lookup_id"));
  }

  @Test
  void testGetFieldsWithoutMeasureFieldName() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.ALL_ROWS);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("");

    IRowMeta inputRowMeta = new RowMeta();
    meta.getFields(inputRowMeta, "FuzzyMatch", null, null, new Variables(), null);

    assertEquals(1, inputRowMeta.size());
    assertEquals("match", inputRowMeta.getValueMeta(0).getName());
  }

  @Test
  void testGetFieldsDistanceMeasureIsInteger() throws Exception {
    for (FuzzyMatchMeta.Algorithm algorithm :
        List.of(
            FuzzyMatchMeta.Algorithm.LEVENSHTEIN,
            FuzzyMatchMeta.Algorithm.DAMERAU_LEVENSHTEIN,
            FuzzyMatchMeta.Algorithm.NEEDLEMAN_WUNSH)) {
      FuzzyMatchMeta meta = new FuzzyMatchMeta();
      meta.setCloserValue(true);
      meta.setAlgorithm(algorithm);
      meta.setOutputMatchField("match");
      meta.setOutputValueField("measure value");

      IRowMeta inputRowMeta = new RowMeta();
      meta.getFields(inputRowMeta, "FuzzyMatch", null, null, new Variables(), null);

      IValueMeta measure = inputRowMeta.searchValueMeta("measure value");
      assertNotNull(measure);
      assertEquals(
          IValueMeta.TYPE_INTEGER,
          measure.getType(),
          "Algorithm " + algorithm + " should output Integer measure value");
    }
  }

  @Test
  void testGetFieldsSimilarityMeasureIsNumber() throws Exception {
    for (FuzzyMatchMeta.Algorithm algorithm :
        List.of(
            FuzzyMatchMeta.Algorithm.JARO,
            FuzzyMatchMeta.Algorithm.JARO_WINKLER,
            FuzzyMatchMeta.Algorithm.PAIR_SIMILARITY)) {
      FuzzyMatchMeta meta = new FuzzyMatchMeta();
      meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
      meta.setAlgorithm(algorithm);
      meta.setOutputMatchField("match");
      meta.setOutputValueField("measure value");

      IRowMeta inputRowMeta = new RowMeta();
      meta.getFields(inputRowMeta, "FuzzyMatch", null, null, new Variables(), null);

      IValueMeta measure = inputRowMeta.searchValueMeta("measure value");
      assertNotNull(measure);
      assertEquals(
          IValueMeta.TYPE_NUMBER,
          measure.getType(),
          "Algorithm " + algorithm + " should output Number measure value");
    }
  }

  @Test
  void testGetFieldsPhoneticMeasureIsString() throws Exception {
    for (FuzzyMatchMeta.Algorithm algorithm :
        List.of(
            FuzzyMatchMeta.Algorithm.METAPHONE,
            FuzzyMatchMeta.Algorithm.DOUBLE_METAPHONE,
            FuzzyMatchMeta.Algorithm.SOUNDEX,
            FuzzyMatchMeta.Algorithm.REFINED_SOUNDEX)) {
      FuzzyMatchMeta meta = new FuzzyMatchMeta();
      meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
      meta.setAlgorithm(algorithm);
      meta.setOutputMatchField("match");
      meta.setOutputValueField("measure value");

      IRowMeta inputRowMeta = new RowMeta();
      meta.getFields(inputRowMeta, "FuzzyMatch", null, null, new Variables(), null);

      IValueMeta measure = inputRowMeta.searchValueMeta("measure value");
      assertNotNull(measure);
      assertEquals(
          IValueMeta.TYPE_STRING,
          measure.getType(),
          "Algorithm " + algorithm + " should output String measure value");
    }
  }

  @Test
  void testGetFieldsThrowsWhenAlgorithmMissing() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.NONE);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");

    assertThrows(
        HopTransformException.class,
        () -> meta.getFields(new RowMeta(), "FuzzyMatch", null, null, new Variables(), null));
  }

  @Test
  void testGetFieldsThrowsWhenLookupValueMissingInInfo() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setMatchMode(FuzzyMatchMeta.MatchMode.CLOSEST);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.LEVENSHTEIN);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("measure value");
    meta.setLookupValues(List.of(new FuzzyMatchMeta.FMLookupValue("missing", null)));

    IRowMeta info = new RowMeta();
    info.addValueMeta(new ValueMetaString("other"));

    assertThrows(
        HopTransformException.class,
        () ->
            meta.getFields(
                new RowMeta(), "FuzzyMatch", new IRowMeta[] {info}, null, new Variables(), null));
  }

  @Test
  void testGetFieldsRename() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setCloserValue(true);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.JARO_WINKLER);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("value");

    String oldName = "old_name";
    String newName = "new_name";
    String noChangeName = "noChangeName";

    FuzzyMatchMeta.FMLookupValue lookupValue = new FuzzyMatchMeta.FMLookupValue(oldName, newName);
    FuzzyMatchMeta.FMLookupValue noChange = new FuzzyMatchMeta.FMLookupValue(noChangeName, null);
    meta.setLookupValues(List.of(lookupValue, noChange));

    IRowMeta inputRowMeta = new RowMeta();
    IRowMeta lookupRowMeta = new RowMeta();
    lookupRowMeta.addValueMeta(new ValueMetaString(oldName));
    lookupRowMeta.addValueMeta(new ValueMetaString(noChangeName));

    meta.getFields(
        inputRowMeta, "FuzzyMatch", new IRowMeta[] {lookupRowMeta}, null, new Variables(), null);

    IValueMeta result = inputRowMeta.searchValueMeta(newName);
    assertNotNull(result);
    assertEquals(newName, result.getName());
    assertEquals("FuzzyMatch", result.getOrigin());

    result = inputRowMeta.searchValueMeta(noChangeName);
    assertNotNull(result);
    assertEquals(noChangeName, result.getName());
  }

  @Test
  void testMatchModeEnumHelpers() {
    assertEquals(3, FuzzyMatchMeta.MatchMode.getDescriptions().length);
    assertEquals(
        FuzzyMatchMeta.MatchMode.ALL_ROWS,
        FuzzyMatchMeta.MatchMode.lookupDescription(
            FuzzyMatchMeta.MatchMode.ALL_ROWS.getDescription()));
    assertEquals(
        FuzzyMatchMeta.MatchMode.ALL_CONCAT, FuzzyMatchMeta.MatchMode.lookupCode("all_concat"));
    assertEquals(FuzzyMatchMeta.MatchMode.CLOSEST, FuzzyMatchMeta.MatchMode.lookupCode("unknown"));
  }

  @Test
  void testAlgorithmEnumHelpers() {
    assertTrue(FuzzyMatchMeta.Algorithm.getDescriptions().length > 0);
    assertEquals(
        FuzzyMatchMeta.Algorithm.LEVENSHTEIN,
        FuzzyMatchMeta.Algorithm.lookupDescription(
            FuzzyMatchMeta.Algorithm.LEVENSHTEIN.getDescription()));
    assertEquals(
        FuzzyMatchMeta.Algorithm.DAMERAU_LEVENSHTEIN,
        FuzzyMatchMeta.Algorithm.lookupCode("dameraulevenshtein"));
    assertEquals(FuzzyMatchMeta.Algorithm.NONE, FuzzyMatchMeta.Algorithm.lookupCode("nope"));
  }

  @Test
  void testFmLookupValueConstructors() {
    FuzzyMatchMeta.FMLookupValue empty = new FuzzyMatchMeta.FMLookupValue();
    assertNull(empty.getName());

    FuzzyMatchMeta.FMLookupValue source = new FuzzyMatchMeta.FMLookupValue("a", "b");
    FuzzyMatchMeta.FMLookupValue copy = new FuzzyMatchMeta.FMLookupValue(source);
    assertEquals("a", copy.getName());
    assertEquals("b", copy.getRename());
  }

  @Test
  void testSupportsErrorHandlingAndExcludeRowLayout() {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    assertTrue(meta.supportsErrorHandling());
    assertTrue(meta.excludeFromRowLayoutVerification());
    assertNotNull(meta.getTransformIOMeta());
    assertEquals(1, meta.getTransformIOMeta().getInfoStreams().size());
  }
}
