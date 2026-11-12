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
package org.apache.hop.pipeline.transforms.language;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.pemistahl.lingua.api.Language;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

class DetectLanguageMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testLoadSave() throws HopException {
    List<String> attributes = List.of("corpusField", "parallelism", "lowAccuracy");

    // the languages list has its own round trip test below
    LoadSaveTester<DetectLanguageMeta> loadSaveTester =
        new LoadSaveTester<>(
            DetectLanguageMeta.class,
            attributes,
            List.of("languages"),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            null);

    loadSaveTester.testSerialization();
  }

  /** A pipeline saved before the language list and the accuracy mode existed. */
  @Test
  void aTransformSavedBeforeTheNewOptionsKeepsItsBehavior() throws Exception {
    DetectLanguageMeta meta =
        load(
            "<transform><corpusField>comment</corpusField><parallelism>Y</parallelism></transform>");

    assertEquals("comment", meta.getCorpusField());
    assertTrue(meta.isParallelism());
    assertFalse(meta.isLowAccuracy());
    assertTrue(meta.getLanguages().isEmpty());
    // no languages listed: every spoken language, as before
    assertTrue(meta.resolveLanguages(new Variables()).isEmpty());
  }

  @Test
  void theNewOptionsSurviveAnXmlRoundTrip() throws Exception {
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.setCorpusField("comment");
    meta.setLowAccuracy(true);
    meta.getLanguages().add(new DetectLanguageChoice("ENGLISH"));
    meta.getLanguages().add(new DetectLanguageChoice("FRENCH"));

    DetectLanguageMeta copy = load("<transform>" + meta.getXml() + "</transform>");

    assertEquals("comment", copy.getCorpusField());
    assertFalse(copy.isParallelism());
    assertTrue(copy.isLowAccuracy());
    assertEquals(
        List.of("ENGLISH", "FRENCH"),
        copy.getLanguages().stream().map(DetectLanguageChoice::getName).toList());
  }

  @Test
  void languagesResolveWithVariablesAnyCaseAndNoDuplicates() throws Exception {
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.getLanguages().add(new DetectLanguageChoice("english"));
    meta.getLanguages().add(new DetectLanguageChoice(" ${SECOND} "));
    meta.getLanguages().add(new DetectLanguageChoice(""));
    meta.getLanguages().add(new DetectLanguageChoice("ENGLISH"));
    Variables variables = new Variables();
    variables.setVariable("SECOND", "French");

    assertEquals(List.of(Language.ENGLISH, Language.FRENCH), meta.resolveLanguages(variables));
  }

  @Test
  void anUnknownLanguageIsRefused() {
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.getLanguages().add(new DetectLanguageChoice("ENGLISH"));
    meta.getLanguages().add(new DetectLanguageChoice("KLINGON"));

    HopException e = assertThrows(HopException.class, () -> meta.resolveLanguages(new Variables()));
    assertTrue(e.getMessage().contains("KLINGON"));
  }

  @Test
  void aSingleLanguageIsRefused() {
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.getLanguages().add(new DetectLanguageChoice("ENGLISH"));

    HopException e = assertThrows(HopException.class, () -> meta.resolveLanguages(new Variables()));
    assertTrue(e.getMessage().contains("ENGLISH"));
  }

  @Test
  void theLanguageListOffersEveryLinguaLanguage() {
    List<String> names = new DetectLanguageMeta().languageNames(null, null);

    assertEquals(Language.all().size(), names.size());
    assertTrue(names.contains("ENGLISH"));
    assertFalse(names.contains("UNKNOWN"));
  }

  @Test
  void checkReportsABadLanguageList() {
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.setCorpusField("comment");
    meta.getLanguages().add(new DetectLanguageChoice("KLINGON"));
    meta.getLanguages().add(new DetectLanguageChoice("ENGLISH"));

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("Detect");
    meta.check(
        remarks,
        null,
        transformMeta,
        null,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        new MemoryMetadataProvider());

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("KLINGON")));
  }

  private static DetectLanguageMeta load(String xml) throws Exception {
    Node node = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    DetectLanguageMeta meta = new DetectLanguageMeta();
    meta.loadXml(node, new MemoryMetadataProvider());
    return meta;
  }
}
