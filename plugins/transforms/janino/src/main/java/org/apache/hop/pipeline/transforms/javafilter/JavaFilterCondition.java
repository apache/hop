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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.janino.function.FunctionLib;
import org.apache.hop.pipeline.transforms.janino.function.HopFunctions;
import org.apache.hop.pipeline.transforms.janino.function.RowAccess;
import org.apache.hop.pipeline.transforms.util.ExpressionValueTypes;
import org.apache.hop.pipeline.transforms.util.JaninoCheckerUtil;
import org.codehaus.janino.ExpressionEvaluator;

/**
 * A compiled Java Filter condition: the Janino expression plus the fields of the stream it needs.
 *
 * <p>Compiling is relatively expensive, so a condition is compiled once (when the first row
 * arrives) and evaluated for every row after that. The dialog compiles one as well, to tell the
 * user whether the condition is valid before the pipeline runs.
 *
 * <p>Fields are bound by name: a field of the stream becomes a parameter of the expression when its
 * name occurs in the condition. Fields that are not mentioned are not passed, which is why a
 * condition can run on a stream that doesn't have them. On top of the fields, the expression always
 * receives a {@link RowAccess} instance named {@value RowAccess#PARAMETER_NAME} to read fields by
 * name at runtime.
 */
public class JavaFilterCondition {

  private final ExpressionEvaluator expressionEvaluator;
  private final List<Integer> argumentIndexes;
  private final List<String> boundFieldNames;
  private final RowAccess rowAccess;
  private final Object[] argumentData;

  private JavaFilterCondition(
      ExpressionEvaluator expressionEvaluator,
      List<Integer> argumentIndexes,
      List<String> boundFieldNames,
      RowAccess rowAccess) {
    this.expressionEvaluator = expressionEvaluator;
    this.argumentIndexes = argumentIndexes;
    this.boundFieldNames = boundFieldNames;
    this.rowAccess = rowAccess;
    this.argumentData = new Object[argumentIndexes.size() + (rowAccess == null ? 0 : 1)];
  }

  /**
   * Compiles a condition against the layout of the stream it will filter.
   *
   * @param rowMeta the layout of the rows the condition will see
   * @param condition the condition, with the variables already resolved
   * @return the compiled condition, ready to evaluate rows
   * @throws HopException when the condition uses code that is not allowed, when it doesn't compile
   *     or when the stream has a field that conflicts with the built-in row helper
   */
  public static JavaFilterCondition compile(IRowMeta rowMeta, String condition)
      throws HopException {
    // The transform accepts any result and reports a non-boolean one per row, with the type it got
    // back.
    return compile(rowMeta, condition, Object.class);
  }

  /**
   * Compiles a condition the way {@link #compile(IRowMeta, String)} does, but also rejects a
   * condition that does not return a boolean. The dialog uses this to tell the user whether the
   * condition is valid before the pipeline runs.
   *
   * @param rowMeta the layout of the rows the condition will see
   * @param condition the condition, with the variables already resolved
   * @return the compiled condition, its bound field names tell which fields of the stream it uses
   * @throws HopException when the condition is not valid
   */
  public static JavaFilterCondition validate(IRowMeta rowMeta, String condition)
      throws HopException {
    return compile(rowMeta, condition, boolean.class);
  }

  private static JavaFilterCondition compile(
      IRowMeta rowMeta, String condition, Class<?> returnType) throws HopException {

    // Only allowed code, the same check the dialog does when the transform is saved.
    //
    JaninoCheckerUtil janinoCheckerUtil = new JaninoCheckerUtil();
    List<String> codeCheck = janinoCheckerUtil.checkCode(condition);
    if (!codeCheck.isEmpty()) {
      throw new HopException("Script contains code that is not allowed : " + codeCheck);
    }

    // Names are looked up in the condition as plain text, so comments are left out: an example in
    // a comment should not bind a field, nor look like a use of the row helper.
    //
    String code = withoutComments(condition);

    List<Integer> argumentIndexes = new ArrayList<>();
    List<String> parameterNames = new ArrayList<>();
    List<Class<?>> parameterTypes = new ArrayList<>();

    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);

      // See if the value is being used in the condition...
      //
      if (code.contains(valueMeta.getName())) {
        // If so, add it to the indexes...
        argumentIndexes.add(i);

        parameterTypes.add(ExpressionValueTypes.javaTypeOf(valueMeta));
        parameterNames.add(valueMeta.getName());
      }
    }

    // A field of the stream with the same name as the row helper wins: it would otherwise be
    // declared twice. The condition can then simply not use the helper.
    //
    RowAccess rowAccess = null;
    if (rowMeta.indexOfValue(RowAccess.PARAMETER_NAME) < 0) {
      rowAccess = new RowAccess();
      parameterNames.add(RowAccess.PARAMETER_NAME);
      parameterTypes.add(RowAccess.class);
    } else if (code.contains(RowAccess.PARAMETER_NAME + ".")) {
      throw new HopException(
          "The stream contains a field named '"
              + RowAccess.PARAMETER_NAME
              + "', which conflicts with the built-in '"
              + RowAccess.PARAMETER_NAME
              + "' helper used in the condition. Rename the field to use "
              + RowAccess.PARAMETER_NAME
              + ".exists() and the other "
              + RowAccess.PARAMETER_NAME
              + " methods.");
    }

    ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
    expressionEvaluator.setParameters(
        parameterNames.toArray(new String[0]), parameterTypes.toArray(new Class<?>[0]));
    expressionEvaluator.setReturnType(returnType);
    expressionEvaluator.setThrownExceptions(new Class<?>[] {Exception.class});
    applyFunctionLibrary(expressionEvaluator);

    try {
      expressionEvaluator.cook(condition);
    } catch (Exception e) {
      throw new HopException("The condition could not be compiled : " + e.getMessage(), e);
    }

    return new JavaFilterCondition(
        expressionEvaluator,
        argumentIndexes,
        new ArrayList<>(parameterNames.subList(0, argumentIndexes.size())),
        rowAccess);
  }

  /**
   * Replaces the Java comments in a condition by spaces, leaving the rest of the text at the same
   * positions. String and character literals are left alone, a // or /* inside one is not a
   * comment.
   *
   * @param condition the condition to clean up
   * @return the condition without its comments
   */
  static String withoutComments(String condition) {
    StringBuilder code = new StringBuilder(condition.length());

    boolean inString = false;
    boolean inChar = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;

    for (int i = 0; i < condition.length(); i++) {
      char c = condition.charAt(i);
      char next = i + 1 < condition.length() ? condition.charAt(i + 1) : 0;

      if (inLineComment) {
        inLineComment = c != '\n';
        code.append(c == '\n' ? c : ' ');
      } else if (inBlockComment) {
        if (c == '*' && next == '/') {
          inBlockComment = false;
          code.append("  ");
          i++;
        } else {
          code.append(Character.isWhitespace(c) ? c : ' ');
        }
      } else if (inString || inChar) {
        code.append(c);
        if (c == '\\' && next != 0) {
          code.append(next);
          i++;
        } else if (inString && c == '"') {
          inString = false;
        } else if (inChar && c == '\'') {
          inChar = false;
        }
      } else if (c == '/' && next == '/') {
        inLineComment = true;
        code.append("  ");
        i++;
      } else if (c == '/' && next == '*') {
        inBlockComment = true;
        code.append("  ");
        i++;
      } else {
        if (c == '"') {
          inString = true;
        } else if (c == '\'') {
          inChar = true;
        }
        code.append(c);
      }
    }
    return code.toString();
  }

  /**
   * Gives the expression the same helper functions the User Defined Java Expression transform has.
   * The plugin class loader and the function library are not available in every context (unit tests
   * for example), the condition is then compiled without them.
   */
  private static void applyFunctionLibrary(ExpressionEvaluator expressionEvaluator) {
    // The helpers that ship with the transform are always available, also when the class path can
    // not be scanned.
    Set<String> imports = new LinkedHashSet<>();
    imports.add(HopFunctions.class.getCanonicalName());
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.getPlugin(TransformPluginType.class, "Janino");
      if (plugin != null) {
        expressionEvaluator.setParentClassLoader(registry.getClassLoader(plugin));
      }
      imports.addAll(Arrays.asList(new FunctionLib().getImportPackages()));
    } catch (Exception e) {
      // No function library on the classpath, the built-in helpers keep working.
    }
    expressionEvaluator.setDefaultImports(imports.toArray(new String[0]));
  }

  /**
   * Evaluates the condition for a single row.
   *
   * @param rowMeta the layout of the row, the same layout the condition was compiled against
   * @param row the row to evaluate
   * @return the outcome of the condition
   * @throws HopException when the expression fails or doesn't return a boolean
   */
  public boolean evaluate(IRowMeta rowMeta, Object[] row) throws HopException {
    for (int x = 0; x < argumentIndexes.size(); x++) {
      int index = argumentIndexes.get(x);
      IValueMeta valueMeta = rowMeta.getValueMeta(index);
      argumentData[x] = valueMeta.convertToNormalStorageType(row[index]);
    }
    if (rowAccess != null) {
      rowAccess.setRow(rowMeta, row);
      argumentData[argumentData.length - 1] = rowAccess;
    }

    Object result;
    try {
      result = expressionEvaluator.evaluate(argumentData);
    } catch (Exception e) {
      throw new HopException(e);
    }

    if (result instanceof Boolean bool) {
      return bool;
    }
    throw new HopException(
        "The result of the filter expression must be a boolean and we got back : "
            + (result == null ? "null" : result.getClass().getName()));
  }

  /** The names of the fields of the stream this condition uses, in stream order. */
  public List<String> getBoundFieldNames() {
    return boundFieldNames;
  }

  /** Whether the condition can use the {@value RowAccess#PARAMETER_NAME} helper. */
  public boolean isRowHelperAvailable() {
    return rowAccess != null;
  }
}
