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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TokenReplacementTest {

  private static final String LETTER = "Dear ${name}, you live in ${city}. ${unknown} ${";

  // More than 500 rows: BaseTransform switches the way it reads rows after 500
  private static final int ROWS = 1_200;

  @TempDir Path tempDir;

  private TransformMockHelper<TokenReplacementMeta, TokenReplacementData> helper;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "Token replacement", TokenReplacementMeta.class, TokenReplacementData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void fillsATemplateTypedInTheTransform() throws Exception {
    List<Object[]> input = new ArrayList<>();
    for (int i = 0; i < ROWS; i++) {
      input.add(new Object[] {"Name " + i, "City " + (i % 7), null});
    }

    List<Object[]> output = run(textToField(LETTER), input, null);

    assertEquals(ROWS, output.size());
    for (int i = 0; i < ROWS; i++) {
      assertEquals(
          "Dear Name " + i + ", you live in City " + (i % 7) + ". ${unknown} ${",
          output.get(i)[3],
          "row " + i);
    }
  }

  @Test
  void resolvesVariablesInFieldValues() throws Exception {
    List<Object[]> input = new ArrayList<>();
    input.add(new Object[] {"${GREETING} there", "100% sure", null});
    input.add(new Object[] {"%%GREETING%%", "$[41,42]", null});
    input.add(new Object[] {"plain text", "${NOT_SET}", null});

    List<Object[]> output =
        run(
            textToField("${name}|${city}"),
            input,
            transform -> transform.setVariable("GREETING", "Hello"));

    assertEquals("Hello there|100% sure", output.get(0)[3]);
    assertEquals("Hello|AB", output.get(1)[3]);
    assertEquals("plain text|${NOT_SET}", output.get(2)[3]);
  }

  @Test
  void anEmptyValueReplacesTheTokenWithNothing() throws Exception {
    List<Object[]> input = new ArrayList<>();
    input.add(new Object[] {null, "", null});

    List<Object[]> output = run(textToField("[${name}][${city}]"), input, null);

    assertEquals("[][]", output.get(0)[3]);
  }

  @Test
  void anEmptyValueAtTheStartKeepsTheRestOfTheText() throws Exception {
    // The old reader gave an empty result for the whole row here
    List<Object[]> input = new ArrayList<>();
    input.add(new Object[] {"", "Ghent", null});

    List<Object[]> output = run(textToField("${name} lives in ${city}"), input, null);

    assertEquals(" lives in Ghent", output.get(0)[3]);
  }

  @Test
  void fillsATemplateFromAField() throws Exception {
    List<Object[]> input = new ArrayList<>();
    for (int i = 0; i < ROWS; i++) {
      // The template changes every few rows
      String template = (i / 3) % 2 == 0 ? "A ${name} in ${city}" : "${city}: B ${name}";
      input.add(new Object[] {"n" + i, "c" + i, template});
    }
    TokenReplacementMeta meta = textToField("not used");
    meta.setInputType("Field");
    meta.setInputFieldName("template");

    List<Object[]> output = run(meta, input, null);

    for (int i = 0; i < ROWS; i++) {
      String expected = (i / 3) % 2 == 0 ? "A n" + i + " in c" + i : "c" + i + ": B n" + i;
      assertEquals(expected, output.get(i)[3], "row " + i);
    }
  }

  @Test
  void writesToAFile() throws Exception {
    List<Object[]> input = new ArrayList<>();
    input.add(new Object[] {"Ann", "Ghent", null});
    input.add(new Object[] {"Bob", "Liège", null});
    TokenReplacementMeta meta = textToField("${name} - ${city}");
    meta.setOutputType("File");
    meta.setOutputFileName(tempDir.resolve("letters.txt").toString());
    meta.setOutputFileFormat("UNIX");
    meta.setOutputFileEncoding("UTF-8");

    List<Object[]> output = run(meta, input, null);

    assertEquals(2, output.size());
    Path written = tempDir.resolve("letters.txt");
    assertTrue(Files.exists(written));
    assertEquals("Ann - Ghent\nBob - Liège\n", Files.readString(written, StandardCharsets.UTF_8));
  }

  @Test
  void readsATemplateFile() throws Exception {
    Path templateFile = tempDir.resolve("template.txt");
    Files.writeString(templateFile, "Hello ${name} from ${city}", StandardCharsets.UTF_8);
    List<Object[]> input = new ArrayList<>();
    input.add(new Object[] {"Ann", "Ghent", null});
    TokenReplacementMeta meta = textToField("not used");
    meta.setInputType("File");
    meta.setInputFileName(templateFile.toString());

    List<Object[]> output = run(meta, input, null);

    assertEquals("Hello Ann from Ghent", output.get(0)[3]);
  }

  @Test
  void onlyExpressionsAreSentToTheVariableResolver() {
    assertTrue(TokenReplacement.mayContainVariables("a ${b}"));
    assertTrue(TokenReplacement.mayContainVariables("%%b%%"));
    assertTrue(TokenReplacement.mayContainVariables("$[41]"));
    assertTrue(TokenReplacement.mayContainVariables("#{a:b}"));
    assertFalse(TokenReplacement.mayContainVariables("100% $ # { } plain"));
  }

  private static TokenReplacementMeta textToField(String template) {
    TokenReplacementMeta meta = new TokenReplacementMeta();
    meta.setInputType("Text");
    meta.setInputText(template);
    meta.setOutputType("Field");
    meta.setOutputFieldName("result");
    List<TokenReplacementField> fields = new ArrayList<>();
    fields.add(field("name", "name"));
    fields.add(field("city", "city"));
    meta.setTokenReplacementFields(fields);
    return meta;
  }

  private static TokenReplacementField field(String name, String tokenName) {
    TokenReplacementField field = new TokenReplacementField();
    field.setName(name);
    field.setTokenName(tokenName);
    return field;
  }

  private List<Object[]> run(
      TokenReplacementMeta meta, List<Object[]> input, Consumer<TokenReplacement> beforeInit)
      throws Exception {
    TokenReplacement transform =
        new TokenReplacement(
            helper.transformMeta,
            meta,
            new TokenReplacementData(),
            0,
            helper.pipelineMeta,
            helper.pipeline);
    if (beforeInit != null) {
      beforeInit.accept(transform);
    }
    transform.init();

    IRowSet rowSet = helper.getMockInputRowSet(input);
    // BaseTransform also reads with getRowImmediate() (every 500 rows): same rows
    when(rowSet.getRowImmediate()).thenAnswer(invocation -> rowSet.getRow());
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("city"));
    rowMeta.addValueMeta(new ValueMetaString("template"));
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    transform.setInputRowSets(new ArrayList<>(List.of(rowSet)));

    List<Object[]> output = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            output.add(row);
          }
        });

    while (transform.processRow()) {
      // keep going until the transform is done
    }
    transform.dispose();
    return output;
  }
}
