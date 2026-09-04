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

package org.apache.hop.pipeline.transforms.janino.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.javafilter.JavaFilterCondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Compiles every entry of the expression reference the editor shows: the syntax that is inserted on
 * a double click and every example in its description. The row layout is the one documented in
 * expressionFunctions.xml.
 */
class ExpressionLibraryTest {

  @BeforeAll
  static void initPlugins() throws Exception {
    HopLogStore.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (String cls :
        new String[] {
          ValueMetaString.class.getName(),
          ValueMetaInteger.class.getName(),
          ValueMetaNumber.class.getName(),
          ValueMetaBigNumber.class.getName(),
          ValueMetaDate.class.getName(),
          ValueMetaBoolean.class.getName(),
          ValueMetaBinary.class.getName(),
          org.apache.hop.core.row.value.ValueMetaTimestamp.class.getName(),
          org.apache.hop.core.row.value.ValueMetaJson.class.getName(),
          org.apache.hop.core.row.value.ValueMetaInternetAddress.class.getName()
        }) {
      registry.registerPluginClass(cls, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  private static IRowMeta referenceRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field"));
    rowMeta.addValueMeta(new ValueMetaInteger("number"));
    rowMeta.addValueMeta(new ValueMetaNumber("value"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("amount"));
    rowMeta.addValueMeta(new ValueMetaDate("date"));
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    rowMeta.addValueMeta(new ValueMetaBinary("binary"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaTimestamp("stamp"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaJson("doc"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaInternetAddress("address"));
    return rowMeta;
  }

  /** Every compilable snippet of the reference: the syntax of an entry and each of its examples. */
  static Stream<String[]> snippets() throws Exception {
    List<String[]> snippets = new ArrayList<>();
    for (FunctionDescription function : ExpressionLibrary.getFunctions()) {
      snippets.add(new String[] {function.getName(), function.getSyntax()});
      for (FunctionExample example : function.getFunctionExamples()) {
        snippets.add(new String[] {function.getName() + " example", example.getExpression()});
      }
    }
    return snippets.stream();
  }

  @Test
  void theReferenceIsRead() throws Exception {
    List<FunctionDescription> functions = ExpressionLibrary.getFunctions();

    assertFalse(functions.isEmpty(), "No expression functions were read");
  }

  @Test
  void everyEntryIsDescribedAndNamedOnlyOnce() throws Exception {
    Set<String> names = new HashSet<>();

    for (FunctionDescription function : ExpressionLibrary.getFunctions()) {
      assertNotNull(function.getName(), "An entry has no name");
      assertNotNull(function.getCategory(), "No category : " + function.getName());
      assertNotNull(function.getDescription(), "No description : " + function.getName());
      assertNotNull(function.getSyntax(), "No syntax : " + function.getName());
      assertNotNull(function.getReturns(), "No return type : " + function.getName());
      // The editor looks an entry up by name when it is selected in the tree.
      assertTrue(names.add(function.getName()), "Duplicate name : " + function.getName());
    }
  }

  @Test
  void theExamplesOfTheJavaFilterAreOnTopOfTheReference() throws Exception {
    List<FunctionDescription> all = ExpressionLibrary.getFunctionsAndConditionExamples();

    assertEquals(
        ExpressionLibrary.getFunctions().size() + ExpressionLibrary.getConditionExamples().size(),
        all.size());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("snippets")
  void snippetCompiles(String name, String snippet) throws Exception {
    // Compiled the way the transforms do it, so a snippet that needs a field, a helper or a fully
    // qualified class name is checked against the real thing.
    JavaFilterCondition.compile(referenceRowMeta(), snippet);
  }
}
