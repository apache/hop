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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.row.IValueMeta;
import org.mozilla.javascript.CompilerEnvirons;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ErrorReporter;
import org.mozilla.javascript.NodeTransformer;
import org.mozilla.javascript.Parser;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ast.ScriptNode;
import org.mozilla.javascript.tools.ToolErrorReporter;

/**
 * Finds the variables a script declares so the "Get variables" button can propose them as output
 * fields.
 *
 * <p>The names come from a static parse of the source, so they are available for every script that
 * compiles, whether or not it can actually run. The types are a best-effort guess read from a scope
 * in which the script was executed against the placeholder input values the dialog generates. That
 * execution is allowed to fail: a script which really inspects its input, say {@code
 * JSON.parse(field)}, throws on placeholder data through no fault of the user, and the variables
 * must still be offered. Anything whose value is unknown keeps the default String type.
 */
final class ScriptValuesVariableDiscovery {

  /** Variables the transform manages itself, never proposed as output fields. */
  private static final Set<String> RESERVED = Set.of("row", "pipeline_status");

  private ScriptValuesVariableDiscovery() {
    // utility class
  }

  /**
   * A variable declared by the script, with the output field type guessed for it. A length or
   * precision of -1 means "not applicable", matching the empty cell the dialog shows.
   */
  record DiscoveredVariable(String name, int type, int length, int precision) {}

  static List<DiscoveredVariable> discover(Context context, Scriptable scope, String script) {
    return discover(context, scope, script, List.of());
  }

  /**
   * @param context Rhino context used to parse the script
   * @param scope scope the script ran in, used to guess types; may be null or only partly filled in
   *     when the script failed halfway
   * @param script the script source
   * @param exclude variable names to leave out, typically the fields already in the grid so that
   *     pressing the button twice does not duplicate them
   */
  static List<DiscoveredVariable> discover(
      Context context, Scriptable scope, String script, Collection<String> exclude) {
    ScriptNode tree = parse(context, script);

    Set<String> seen = new LinkedHashSet<>(exclude);
    List<DiscoveredVariable> variables = new ArrayList<>();
    for (int i = 0; i < tree.getParamAndVarCount(); i++) {
      String name = tree.getParamOrVarName(i);
      if (RESERVED.contains(name.toLowerCase()) || !seen.add(name)) {
        continue;
      }
      variables.add(describe(name, scope == null ? null : scope.get(name, scope)));
    }
    return variables;
  }

  /** Parse the source into an AST without running it. */
  static ScriptNode parse(Context context, String script) {
    CompilerEnvirons environment = new CompilerEnvirons();
    environment.initFromContext(context);
    environment.setOptimizationLevel(-1);
    environment.setGeneratingSource(true);
    environment.setGenerateDebugInfo(true);
    ErrorReporter errorReporter = new ToolErrorReporter(false);
    Parser parser = new Parser(environment, errorReporter);
    ScriptNode tree = parser.parse(script, "", 0);
    new NodeTransformer().transform(tree, environment);
    return tree;
  }

  /**
   * Map the Java wrapper Rhino used for a value onto a Hop type. Anything unrecognised stays a
   * String: that covers NOT_FOUND and Undefined, which is what a variable looks like when the
   * script never ran or never reached its assignment.
   */
  private static DiscoveredVariable describe(String name, Object value) {
    if (value == null) {
      return new DiscoveredVariable(name, IValueMeta.TYPE_STRING, -1, -1);
    }
    return switch (value.getClass().getName()) {
        // MAX = 127
      case "java.lang.Byte" -> new DiscoveredVariable(name, IValueMeta.TYPE_INTEGER, 3, 0);
        // MAX = 2147483647
      case "java.lang.Integer" -> new DiscoveredVariable(name, IValueMeta.TYPE_INTEGER, 9, 0);
        // MAX = 9223372036854775807
      case "java.lang.Long" -> new DiscoveredVariable(name, IValueMeta.TYPE_INTEGER, 18, 0);
      case "java.lang.Double" -> new DiscoveredVariable(name, IValueMeta.TYPE_NUMBER, 16, 2);
      case "org.mozilla.javascript.NativeDate", "java.util.Date" ->
          new DiscoveredVariable(name, IValueMeta.TYPE_DATE, -1, -1);
      case "java.lang.Boolean" -> new DiscoveredVariable(name, IValueMeta.TYPE_BOOLEAN, -1, -1);
      default -> new DiscoveredVariable(name, IValueMeta.TYPE_STRING, -1, -1);
    };
  }
}
