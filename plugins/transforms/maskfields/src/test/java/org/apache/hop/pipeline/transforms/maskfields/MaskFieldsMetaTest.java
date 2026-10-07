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

package org.apache.hop.pipeline.transforms.maskfields;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class MaskFieldsMetaTest {

  @Test
  void rejectsPatternsThatDoNotFitTheField() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    save(provider, synthetic("Dates", MaskingToken.SEQUENCE));
    save(provider, synthetic("Ids", MaskingToken.UUID));
    MaskingPattern blank = new MaskingPattern();
    blank.setName("Blank");
    blank.setValueSource(MaskingValueSource.SET_EMPTY);
    save(provider, blank);
    save(provider, synthetic("Names", MaskingToken.SEQUENCE));

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("born", "Dates"));
    meta.getFields().add(new MaskField("id", "Ids"));
    meta.getFields().add(new MaskField("born", "Blank"));
    meta.getFields().add(new MaskField("name", "Missing"));

    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaDate("born"));
    prev.addValueMeta(new ValueMetaInteger("id"));
    prev.addValueMeta(new ValueMetaString("name"));

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transform = new TransformMeta();
    transform.setName("Mask");
    meta.check(
        remarks,
        null,
        transform,
        prev,
        new String[] {"in"},
        new String[0],
        null,
        new Variables(),
        provider);

    assertTrue(
        messages(remarks).stream()
            .anyMatch(text -> text.contains("born") && text.contains("Dates")));
    assertTrue(messages(remarks).stream().anyMatch(text -> text.contains("UUID")));
    assertTrue(messages(remarks).stream().anyMatch(text -> text.contains("blank")));
    assertTrue(messages(remarks).stream().anyMatch(text -> text.contains("Missing")));
  }

  @Test
  void warnsAboutPlainTextKeysAndLongReplacements() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    MaskingPattern plain = synthetic("Plain", MaskingToken.SEQUENCE);
    plain.setStorage(MaskingStorage.DATABASE);
    plain.setConnection("db");
    plain.setTableName("mask_map");
    save(provider, plain);
    MaskingPattern hashed = synthetic("Hashed", MaskingToken.UUID);
    hashed.setStorage(MaskingStorage.DATABASE);
    hashed.setConnection("db");
    hashed.setTableName("mask_map");
    hashed.setHashSecret("${SECRET}");
    hashed.setPrefix("${PREFIX}");
    save(provider, hashed);

    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("name", "Plain"));
    meta.getFields().add(new MaskField("city", "Hashed"));
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("name"));
    prev.addValueMeta(new ValueMetaString("city"));
    Variables variables = new Variables();
    variables.setVariable("PREFIX", "x".repeat(250));

    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transform = new TransformMeta();
    transform.setName("Mask");
    meta.check(
        remarks,
        null,
        transform,
        prev,
        new String[] {"in"},
        new String[0],
        null,
        variables,
        provider);

    List<String> warnings =
        remarks.stream()
            .filter(remark -> remark.getType() == ICheckResult.TYPE_RESULT_WARNING)
            .map(ICheckResult::getText)
            .toList();
    assertEquals(2, warnings.size(), warnings.toString());
    assertTrue(warnings.get(0).contains("Plain") && warnings.get(0).contains("plain text"));
    assertTrue(warnings.get(1).contains("Hashed") && warnings.get(1).contains("286"));
  }

  @Test
  void roundTripsTheTransformAndThePattern() throws Exception {
    MaskFieldsMeta meta = new MaskFieldsMeta();
    meta.getFields().add(new MaskField("name", "First name"));
    String xml = meta.getXml();
    Node node = XmlHandler.loadXmlString("<transform>" + xml + "</transform>").getDocumentElement();
    MaskFieldsMeta copy = new MaskFieldsMeta();
    copy.loadXml(node, new MemoryMetadataProvider());
    assertEquals("name", copy.getFields().get(0).getFieldName());
    assertEquals("First name", copy.getFields().get(0).getPatternName());

    MaskingPattern pattern = synthetic("First name", MaskingToken.SEQUENCE);
    pattern.setPrefix("first-name-");
    pattern.setPiiClassification("Direct identifier");
    pattern.setStorage(MaskingStorage.MEMORY);
    pattern.setTrimKey(true);
    pattern.setIgnoreCase(true);
    pattern.setHashSecret("s3cret");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    JsonMetadataParser<MaskingPattern> parser =
        new JsonMetadataParser<>(MaskingPattern.class, provider);
    JSONObject json = parser.getJsonObject(pattern);
    try (JsonParser jsonParser = new JsonFactory().createParser(json.toJSONString())) {
      jsonParser.nextToken();
      MaskingPattern loaded = parser.loadJsonObject(MaskingPattern.class, jsonParser);
      assertEquals("First name", loaded.getName());
      assertEquals("first-name-", loaded.getPrefix());
      assertEquals("Direct identifier", loaded.getPiiClassification());
      assertEquals(MaskingStorage.MEMORY, loaded.getStorage());
      assertEquals(MaskingValueSource.SYNTHETIC, loaded.getValueSource());
      assertTrue(loaded.isTrimKey());
      assertTrue(loaded.isIgnoreCase());
      assertEquals("s3cret", loaded.getHashSecret());
      assertFalse(json.toJSONString().contains("s3cret"));
      assertNull(MaskingRules.incompatibility(new ValueMetaString("name"), loaded));
    }
  }

  private static MaskingPattern synthetic(String name, MaskingToken token) {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName(name);
    pattern.setValueSource(MaskingValueSource.SYNTHETIC);
    pattern.setToken(token);
    pattern.setPrefix(token == MaskingToken.UUID ? "" : "n-");
    pattern.setSequenceStart("1");
    return pattern;
  }

  private static void save(MemoryMetadataProvider provider, MaskingPattern pattern)
      throws Exception {
    provider.getSerializer(MaskingPattern.class).save(pattern);
  }

  private static List<String> messages(List<ICheckResult> remarks) {
    List<String> messages = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        messages.add(remark.getText());
      }
    }
    assertTrue(remarks.stream().allMatch(remark -> remark instanceof CheckResult));
    return messages;
  }
}
