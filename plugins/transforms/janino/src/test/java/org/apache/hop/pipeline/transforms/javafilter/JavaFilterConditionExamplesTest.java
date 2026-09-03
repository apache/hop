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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.janino.function.ExpressionLibrary;
import org.apache.hop.pipeline.transforms.janino.function.FunctionDescription;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Compiles every example condition the dialog offers, so an example can not drift away from what
 * the transform accepts. The row layout mirrors the one documented in conditionExamples.xml.
 */
class JavaFilterConditionExamplesTest {

  @BeforeAll
  static void initPlugins() throws Exception {
    HopLogStore.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (String cls :
        new String[] {
          ValueMetaString.class.getName(),
          ValueMetaInteger.class.getName(),
          ValueMetaDate.class.getName(),
          ValueMetaNumber.class.getName(),
          ValueMetaBoolean.class.getName()
        }) {
      registry.registerPluginClass(cls, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  private static IRowMeta exampleRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("group"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaNumber("price"));
    rowMeta.addValueMeta(new ValueMetaDate("order_date"));
    rowMeta.addValueMeta(new ValueMetaBoolean("active"));
    return rowMeta;
  }

  static Stream<FunctionDescription> examples() throws Exception {
    return ExpressionLibrary.getConditionExamples().stream();
  }

  @Test
  void examplesAreRead() throws Exception {
    List<FunctionDescription> examples = ExpressionLibrary.getConditionExamples();

    assertFalse(examples.isEmpty(), "No condition examples were read");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("examples")
  void exampleIsDescribed(FunctionDescription example) {
    assertNotNull(example.getName(), "An example has no name");
    assertNotNull(example.getCategory(), "Example has no category : " + example.getName());
    assertNotNull(example.getDescription(), "Example has no description : " + example.getName());
    assertNotNull(example.getSyntax(), "Example has no expression : " + example.getName());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("examples")
  void exampleCompilesAndReturnsABoolean(FunctionDescription example) throws Exception {
    // Variables are resolved before the condition is compiled, exactly like the transform does it.
    Variables variables = new Variables();
    variables.setVariable("GROUP", "A");
    variables.setVariable("CONDITION", "true");

    JavaFilterCondition condition =
        JavaFilterCondition.validate(
            exampleRowMeta(), variables.resolve(example.getSyntax().trim()));

    assertTrue(
        condition.getBoundFieldNames().size() <= exampleRowMeta().size(),
        "Example binds unknown fields : " + example.getName());
  }
}
