/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.Test;

class FilenamePatternSyntaxTest {
  private boolean matches(String wildcard, String filename) {
    return Pattern.compile(FilenamePatternSyntax.WILDCARD.toRegex(wildcard))
        .matcher(filename)
        .matches();
  }

  @Test
  void wildcardStarMatchesAllAndContainsPatternsMatchWholeFilenames() {
    assertTrue(matches("*", "datos.csv"));
    assertTrue(matches("*", "árbol\n😀.txt"));
    assertTrue(matches("*test*", "pre-test-post.csv"));
    assertTrue(matches("test*", "test.csv"));
    assertTrue(matches("test*", "test"));
    assertFalse(matches("test*", "pre-test.csv"));
    assertTrue(matches("*test", "pre-test"));
    assertTrue(matches("*test", "test"));
    assertFalse(matches("*test", "test.csv"));
    assertTrue(matches("*test.txt", "pre-test.txt"));
    assertFalse(matches("*test.txt", "pre-test.txt.bak"));
    assertFalse(matches("*test*", "data.csv"));
    assertFalse(matches("*test*", "TEST.csv"));
    assertTrue(matches("*.csv", "data.csv"));
    assertFalse(matches("*.csv", "dataXcsv"));
  }

  @Test
  void wildcardQuestionMatchesOneCharacterAndRegexCharactersAreLiteral() {
    assertTrue(matches("file?.txt", "file1.txt"));
    assertTrue(matches("file?.txt", "file😀.txt"));
    assertFalse(matches("file?.txt", "file12.txt"));
    assertTrue(matches("file[1](a)+$.txt", "file[1](a)+$.txt"));
    assertFalse(matches("file[1](a)+$.txt", "file1a.txt"));
    assertTrue(matches("árbol😀.*", "árbol😀.csv"));
    assertTrue(matches("\\E*", "\\Etest"));
  }

  @Test
  void regexpAndEmptyPatternsRetainTheirOriginalMeaning() {
    assertEquals("", FilenamePatternSyntax.WILDCARD.toRegex(null));
    assertEquals("", FilenamePatternSyntax.WILDCARD.toRegex(""));
    String expression = ".*\\.(csv|txt)";
    assertEquals(expression, FilenamePatternSyntax.REGEXP.toRegex(expression));
    assertThrows(
        java.util.regex.PatternSyntaxException.class,
        () -> Pattern.compile(FilenamePatternSyntax.REGEXP.toRegex("*")));
  }

  @Test
  void oldXmlRetainsRegexpWhileNewGuiDefaultsUseWildcardsAndVariablesResolve() throws Exception {
    WatchFilesMeta legacy =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString(
                    "<transform><includeWildcard>.*\\.csv</includeWildcard></transform>"),
                "transform"),
            WatchFilesMeta.class,
            new MemoryMetadataProvider());
    Variables variables = new Variables();
    assertEquals("REGEXP", legacy.getPatternSyntax());
    assertEquals(".*\\.csv", legacy.filenameRegex(variables, legacy.getIncludeWildcard()));
    WatchFilesMeta fresh = new WatchFilesMeta();
    fresh.setDefault();
    assertEquals("WILDCARD", fresh.getPatternSyntax());
    variables.setVariable("INCLUDE", "*test*");
    assertTrue(
        Pattern.compile(fresh.filenameRegex(variables, "${INCLUDE}"))
            .matcher("a-test.txt")
            .matches());
  }
}
