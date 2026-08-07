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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassCodeSnippets.Snippet;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassDef.ClassType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Compiles every snippet offered by the User Defined Java Class dialog with the very same Janino
 * setup the transform itself uses, so the snippets can no longer rot away from the API unnoticed
 * (issue #2718).
 *
 * <p>Snippets come in two shapes. A snippet that declares a class member is compiled as is. A
 * snippet that is a statement fragment is compiled inside a harness method which also declares the
 * placeholder names the fragments are written against.
 */
class UserDefinedJavaClassCodeSnippetsCompileTest {

  /**
   * Placeholder fields the statement fragments are written against. A fragment declaring a local
   * variable with the same name simply shadows the field.
   */
  private static final String PLACEHOLDERS =
      """
      Object[] r;
      IRowSet rowSet;
      String msg;
      IRowListener rowListener;
      """;

  private static final String PROCESS_ROW_STUB =
      "public boolean processRow() throws HopException { return false; }\n";

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  static Stream<Arguments> codeBlocks() throws Exception {
    return blocks(false);
  }

  static Stream<Arguments> sampleBlocks() throws Exception {
    return blocks(true);
  }

  private static Stream<Arguments> blocks(boolean samples) throws Exception {
    List<Snippet> snippets = UserDefinedJavaClassCodeSnippets.getSnippetsHelper().getSnippets();
    if (snippets.isEmpty()) {
      fail("No code snippets were loaded from codeSnippets.xml");
    }
    return snippets.stream()
        .map(
            snippet ->
                Arguments.of(snippet.getName(), samples ? snippet.getSample() : snippet.getCode()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("codeBlocks")
  void snippetCodeCompiles(String name, String code) {
    assertNotNull(code, "Snippet '" + name + "' has no code");
    compile(name, code);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("sampleBlocks")
  void snippetSampleCompiles(String name, String sample) {
    assertNotNull(sample, "Snippet '" + name + "' has no sample");
    compile(name, sample);
  }

  private void compile(String name, String code) {
    String source = isClassMember(code) ? asClassMember(code) : asStatementFragment(code);
    UserDefinedJavaClassDef def =
        new UserDefinedJavaClassDef(ClassType.TRANSFORM_CLASS, className(name), source);
    try {
      assertNotNull(
          new UserDefinedJavaClassMeta()
              .cookClass(def, UserDefinedJavaClass.class.getClassLoader()));
    } catch (Exception e) {
      fail("Snippet '" + name + "' does not compile: " + e.getMessage() + "\n\n" + source, e);
    }
  }

  private String asClassMember(String code) {
    return code.contains("boolean processRow()") ? code : code + "\n" + PROCESS_ROW_STUB;
  }

  private String asStatementFragment(String code) {
    return PLACEHOLDERS
        + "\npublic boolean snippetHarness() throws Exception {\n"
        + code
        + "\nreturn true;\n}\n"
        + PROCESS_ROW_STUB;
  }

  /**
   * A snippet declares a class member when its first line of actual code starts with a modifier.
   */
  private boolean isClassMember(String code) {
    for (String line : code.split("\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()
          || trimmed.startsWith("//")
          || trimmed.startsWith("/*")
          || trimmed.startsWith("*")) {
        continue;
      }
      return trimmed.startsWith("public ")
          || trimmed.startsWith("private ")
          || trimmed.startsWith("protected ");
    }
    return false;
  }

  private String className(String snippetName) {
    return "Snippet" + snippetName.replaceAll("[^A-Za-z0-9]", "");
  }
}
