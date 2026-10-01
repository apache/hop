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

package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.javascript.ScriptValuesVariableDiscovery.DiscoveredVariable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EvaluatorException;
import org.mozilla.javascript.RhinoException;
import org.mozilla.javascript.Scriptable;

/**
 * Covers the variable discovery behind the "Get variables" button of the JavaScript transform
 * dialog.
 */
class ScriptValuesVariableDiscoveryTest {

  /**
   * The placeholder ScriptValuesDialog binds to every incoming String field so it can run the
   * script without real data.
   */
  private static final String SAMPLE_STRING_VALUE =
      "test value test value test value test value test value "
          + "test value test value test value test value test value";

  private Context cx;
  private Scriptable scope;

  @BeforeEach
  void enterRhino() {
    cx = Context.enter();
    cx.setOptimizationLevel(-1);
    scope = cx.initStandardObjects();
  }

  @AfterEach
  void exitRhino() {
    Context.exit();
  }

  private List<DiscoveredVariable> discover(String script) {
    return ScriptValuesVariableDiscovery.discover(cx, scope, script);
  }

  private static List<String> namesOf(List<DiscoveredVariable> variables) {
    return variables.stream().map(DiscoveredVariable::name).toList();
  }

  private static Map<String, DiscoveredVariable> byName(List<DiscoveredVariable> variables) {
    return variables.stream()
        .collect(Collectors.toMap(DiscoveredVariable::name, Function.identity()));
  }

  @Test
  @DisplayName("issue #3403: variables are found even though the script cannot run on sample data")
  void variablesAreFoundWhenScriptFailsOnSampleData() {
    // The script from the issue report. It is perfectly valid, but it parses its input field, so
    // it always blows up on the placeholder value the dialog feeds it.
    //
    String script =
        """
        //Script to flatten JSON keys for Hop

        var input_json = JSON.parse(tabsFlat);
        var output_json = [];

        for (var key in input_json) {
            var value = input_json[key];
            output_json.push({
                field1: key,
                field2: value
            });
        }

        var flattened_json = JSON.stringify(output_json);
        """;

    scope.put("tabsFlat", scope, Context.toObject(SAMPLE_STRING_VALUE, scope));

    // The script compiles: nothing is wrong with it.
    //
    var compiled = cx.compileString(script, "script", 1, null);

    // Running it on the placeholder value is what fails, and it is not the user's fault.
    //
    RhinoException failure = assertThrows(RhinoException.class, () -> compiled.exec(cx, scope));
    assertTrue(
        failure.details().contains("Unexpected token"),
        "expected the JSON.parse failure from the issue, got: " + failure.details());

    // The variables must be offered regardless. Before the fix this returned nothing at all and
    // the user had to type flattened_json by hand.
    //
    assertEquals(
        List.of("input_json", "output_json", "key", "value", "flattened_json"),
        namesOf(discover(script)));
  }

  @Test
  @DisplayName("a script that never ran yields String typed variables rather than none")
  void unknownValuesFallBackToString() {
    List<DiscoveredVariable> variables = discover("var a; var b = 1; var c = 'x';");

    // Nothing was executed, so no value is known for any of them.
    //
    for (DiscoveredVariable variable : variables) {
      assertEquals(IValueMeta.TYPE_STRING, variable.type(), variable.name());
      assertEquals(-1, variable.length(), variable.name());
      assertEquals(-1, variable.precision(), variable.name());
    }
    assertEquals(List.of("a", "b", "c"), namesOf(variables));
  }

  @Test
  @DisplayName("types are guessed from the scope when the script did run")
  void typesAreReadFromTheExecutedScope() {
    String script =
        """
        var aNumber = 42.5;
        var aBoolean = true;
        var aDate = new Date();
        var aString = 'text';
        var neverAssigned;
        """;
    cx.evaluateString(scope, script, "script", 1, null);

    Map<String, DiscoveredVariable> variables = byName(discover(script));

    assertEquals(IValueMeta.TYPE_NUMBER, variables.get("aNumber").type());
    assertEquals(16, variables.get("aNumber").length());
    assertEquals(2, variables.get("aNumber").precision());

    assertEquals(IValueMeta.TYPE_BOOLEAN, variables.get("aBoolean").type());
    assertEquals(IValueMeta.TYPE_DATE, variables.get("aDate").type());
    assertEquals(IValueMeta.TYPE_STRING, variables.get("aString").type());
    assertEquals(IValueMeta.TYPE_STRING, variables.get("neverAssigned").type());
  }

  @Test
  @DisplayName("integral java values coming back from java calls map onto Integer fields")
  void javaIntegralTypesMapOntoIntegerFields() {
    String script = "var aByte; var anInteger; var aLong;";
    scope.put("aByte", scope, Byte.valueOf((byte) 1));
    scope.put("anInteger", scope, Integer.valueOf(2));
    scope.put("aLong", scope, Long.valueOf(3L));

    Map<String, DiscoveredVariable> variables = byName(discover(script));

    assertEquals(IValueMeta.TYPE_INTEGER, variables.get("aByte").type());
    assertEquals(3, variables.get("aByte").length());
    assertEquals(0, variables.get("aByte").precision());

    assertEquals(IValueMeta.TYPE_INTEGER, variables.get("anInteger").type());
    assertEquals(9, variables.get("anInteger").length());

    assertEquals(IValueMeta.TYPE_INTEGER, variables.get("aLong").type());
    assertEquals(18, variables.get("aLong").length());
  }

  @Test
  @DisplayName("the transform's own variables are never proposed as output fields")
  void reservedVariablesAreSkipped() {
    assertEquals(List.of("mine"), namesOf(discover("var row; var pipeline_Status; var mine = 1;")));
  }

  @Test
  @DisplayName("fields already in the grid are not proposed again")
  void alreadyPresentVariablesAreSkipped() {
    String script = "var first = 1; var second = 2; var third = 3;";

    List<DiscoveredVariable> variables =
        ScriptValuesVariableDiscovery.discover(cx, scope, script, List.of("first", "third"));

    assertEquals(List.of("second"), namesOf(variables));
  }

  @Test
  @DisplayName("a variable declared more than once is proposed once")
  void repeatedDeclarationsAreCollapsed() {
    assertEquals(List.of("total"), namesOf(discover("var total = 1; var total = 2;")));
  }

  @Test
  @DisplayName("a genuine syntax error is still reported")
  void syntaxErrorsStillFail() {
    assertThrows(EvaluatorException.class, () -> discover("var broken = {;"));
  }
}
