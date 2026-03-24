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

package org.apache.hop.pipeline.transforms.fileinput.text;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TextFileInputUtilsTest {
  @Test
  void guessStringsFromLine() throws Exception {
    TextFileInputMeta inputMeta = new TextFileInputMeta();
    inputMeta.setContent(new TextFileInputMeta.Content());
    inputMeta.getContent().setFileType("CSV");

    String line =
        "\"\\\\valueA\"|\"valueB\\\\\"|\"val\\\\ueC\""; // "\\valueA"|"valueB\\"|"val\\ueC"

    String[] strings =
        TextFileInputUtils.guessStringsFromLine(
            Mockito.mock(IVariables.class),
            Mockito.mock(ILogChannel.class),
            line,
            inputMeta,
            "|",
            "\"",
            "\\");
    assertNotNull(strings);
    assertEquals("\\valueA", strings[0]);
    assertEquals("valueB\\", strings[1]);
    assertEquals("val\\ueC", strings[2]);
  }

  @Test
  void convertLineToStrings() throws Exception {
    TextFileInputMeta inputMeta = new TextFileInputMeta();
    inputMeta.setContent(new TextFileInputMeta.Content());
    inputMeta.getContent().setFileType("CSV");
    inputMeta.setInputFields(
        List.of(
            new TextFileInputField("one"),
            new TextFileInputField("two"),
            new TextFileInputField("three")));
    inputMeta.getContent().setEscapeCharacter("\\");

    String line =
        "\"\\\\fie\\\\l\\dA\"|\"fieldB\\\\\"|\"fie\\\\ldC\""; // ""\\fie\\l\dA"|"fieldB\\"|"Fie\\ldC""

    String[] strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, "|", "\"", "\\");
    assertNotNull(strings);
    assertEquals("\\fie\\l\\dA", strings[0]);
    assertEquals("fieldB\\", strings[1]);
    assertEquals("fie\\ldC", strings[2]);
  }

  @Test
  void convertCSVLinesToStrings() throws Exception {
    TextFileInputMeta inputMeta = new TextFileInputMeta();
    inputMeta.setContent(new TextFileInputMeta.Content());
    inputMeta.getContent().setFileType("CSV");
    inputMeta.setInputFields(List.of(new TextFileInputField("one"), new TextFileInputField("two")));
    inputMeta.getContent().setEscapeCharacter("\\");

    String line = "A\\\\,B"; // A\\,B

    String[] strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "", "\\");
    assertNotNull(strings);
    assertEquals("A\\", strings[0]);
    assertEquals("B", strings[1]);

    line = "\\,AB"; // \,AB

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "", "\\");
    assertNotNull(strings);
    assertEquals(",AB", strings[0]);
    assertEquals(null, strings[1]);

    line = "\\\\\\,AB"; // \\\,AB

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "", "\\");
    assertNotNull(strings);
    assertEquals("\\,AB", strings[0]);
    assertEquals(null, strings[1]);

    line = "AB,\\"; // AB,\

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "", "\\");
    assertNotNull(strings);
    assertEquals("AB", strings[0]);
    assertEquals("\\", strings[1]);

    line = "AB,\\\\\\"; // AB,\\\

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "", "\\");
    assertNotNull(strings);
    assertEquals("AB", strings[0]);
    assertEquals("\\\\", strings[1]);

    line = "A\\B,C"; // A\B,C

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "", "\\");
    assertNotNull(strings);
    assertEquals("A\\B", strings[0]);
    assertEquals("C", strings[1]);
  }

  @Test
  void convertCSVLinesToStringsWithEnclosure() throws Exception {
    TextFileInputMeta inputMeta = new TextFileInputMeta();
    inputMeta.setContent(new TextFileInputMeta.Content());
    inputMeta.getContent().setFileType("CSV");
    inputMeta.setInputFields(List.of(new TextFileInputField("one"), new TextFileInputField("two")));
    inputMeta.getContent().setEscapeCharacter("\\");
    inputMeta.getContent().setEnclosure("\"");

    String line = "\"A\\\\\",\"B\""; // "A\\","B"

    String[] strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "\"", "\\");
    assertNotNull(strings);
    assertEquals("A\\", strings[0]);
    assertEquals("B", strings[1]);

    line = "\"\\\\\",\"AB\""; // "\\","AB"

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "\"", "\\");
    assertNotNull(strings);
    assertEquals("\\", strings[0]);
    assertEquals("AB", strings[1]);

    line = "\"A\\B\",\"C\""; // "A\B","C"

    strings =
        TextFileInputUtils.convertLineToStrings(
            Mockito.mock(ILogChannel.class), line, inputMeta, ",", "\"", "\\");
    assertNotNull(strings);
    assertEquals("A\\B", strings[0]);
    assertEquals("C", strings[1]);
  }
}
