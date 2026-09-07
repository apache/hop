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
package org.apache.hop.pipeline.transforms.formula.fast;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.formula.fast.FastFormulaEvaluator.Node;
import org.apache.hop.pipeline.transforms.formula.fast.FastFormulaEvaluator.UnsupportedFormulaException;

/**
 * Decides whether a formula can run on the plain-Java fast path and, when it can, compiles it once
 * into a reusable {@link Function} and caches the result.
 *
 * <p>The cache is a fixed-size LRU keyed on the resolved formula together with the value types of
 * the fields it references and its {@code setNa} flag. Both the compile cost and the eligibility
 * decision are cached, so a pipeline only pays the parse once per distinct formula.
 *
 * <p>The maximum number of cached entries is a {@code -D} property, {@value #MAX_SIZE_PROPERTY},
 * and defaults to {@value #DEFAULT_MAX_SIZE}. The whole fast path can be disabled with {@value
 * #ENABLED_PROPERTY}, which makes {@link #compile} always report "not eligible".
 */
public final class FastFormulaCompiler {

  public static final String MAX_SIZE_PROPERTY =
      "org.apache.hop.pipeline.transforms.formula.fast.FastFormulaCompiler.maxSize";
  public static final String ENABLED_PROPERTY =
      "org.apache.hop.pipeline.transforms.formula.fast.FastFormulaCompiler.enabled";
  public static final int DEFAULT_MAX_SIZE = 1024;

  /** Binds a formula to its compiled function (or states it is not eligible). */
  public record CompiledFormula(boolean fastPath, Function<Object[], Object> function) {
    public static final CompiledFormula NOT_ELIGIBLE = new CompiledFormula(false, null);
  }

  /** Sentinel put in the args array for a null field bound with the "#N/A" option. */
  public static final Object NA = new Object();

  private static final Map<String, CompiledFormula> CACHE = createLru(maxSize());

  private FastFormulaCompiler() {}

  /**
   * Returns the cached result for a formula, computing and caching it when not present.
   *
   * @param resolvedFormula the variable-resolved formula, ready to be parsed
   * @param fieldNames the fields the formula references, in argument order
   * @param rowMeta the row metadata, used for the value types of the fields
   * @param setNa whether null fields are turned into {@code #N/A}
   * @return the compiled formula, whose {@link CompiledFormula#fastPath()} is false when the
   *     formula is out of the fast-path subset or the fast path is disabled
   */
  public static CompiledFormula compile(
      String resolvedFormula, List<String> fieldNames, IRowMeta rowMeta, boolean setNa) {
    if (!isEnabled()) {
      return CompiledFormula.NOT_ELIGIBLE;
    }
    String key = key(resolvedFormula, fieldNames, rowMeta, setNa);
    synchronized (CACHE) {
      CompiledFormula cached = CACHE.get(key);
      if (cached != null) {
        return cached;
      }
    }
    CompiledFormula compiled = compileUncached(resolvedFormula, fieldNames, rowMeta, setNa);
    synchronized (CACHE) {
      CACHE.put(key, compiled);
    }
    return compiled;
  }

  private static CompiledFormula compileUncached(
      String resolvedFormula, List<String> fieldNames, IRowMeta rowMeta, boolean setNa) {
    if (!eligibleTypes(fieldNames, rowMeta)) {
      return CompiledFormula.NOT_ELIGIBLE;
    }
    Map<String, Integer> fieldIndex = new LinkedHashMap<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      fieldIndex.putIfAbsent(fieldNames.get(i), i);
    }
    final Node root;
    try {
      root = FastFormulaEvaluator.parse(resolvedFormula, fieldIndex);
    } catch (UnsupportedFormulaException e) {
      return CompiledFormula.NOT_ELIGIBLE;
    }
    // The function is a thin closure over the already-parsed tree: calling it only runs the tree.
    Function<Object[], Object> function = root::eval;
    return new CompiledFormula(true, function);
  }

  /** Fields of these value types can be read natively by the fast path. */
  private static boolean eligibleTypes(List<String> fieldNames, IRowMeta rowMeta) {
    for (String fieldName : fieldNames) {
      int type = rowMeta.getValueMeta(rowMeta.indexOfValue(fieldName)).getType();
      if (!isFastType(type)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isFastType(int type) {
    switch (type) {
      case IValueMeta.TYPE_STRING,
          IValueMeta.TYPE_NUMBER,
          IValueMeta.TYPE_INTEGER,
          IValueMeta.TYPE_BIGNUMBER,
          IValueMeta.TYPE_BOOLEAN:
        return true;
      default:
        return false;
    }
  }

  private static String key(
      String resolvedFormula, List<String> fieldNames, IRowMeta rowMeta, boolean setNa) {
    StringBuilder key = new StringBuilder(resolvedFormula.length() + fieldNames.size() * 12 + 8);
    key.append(setNa ? "na" : "plain").append('=').append(resolvedFormula);
    for (String fieldName : fieldNames) {
      key.append('|').append(fieldName);
      key.append(':').append(rowMeta.getValueMeta(rowMeta.indexOfValue(fieldName)).getType());
    }
    return key.toString();
  }

  private static boolean isEnabled() {
    return Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
  }

  private static int maxSize() {
    int size = Integer.getInteger(MAX_SIZE_PROPERTY, DEFAULT_MAX_SIZE);
    return Math.max(1, size);
  }

  private static Map<String, CompiledFormula> createLru(int maxSize) {
    return new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, CompiledFormula> eldest) {
        return size() > maxSize;
      }
    };
  }

  /** Clears the cache. Intended for tests. */
  public static void clear() {
    synchronized (CACHE) {
      CACHE.clear();
    }
  }

  /** The current size of the cache (when the max size was read at class load). */
  static int cacheSize() {
    synchronized (CACHE) {
      return CACHE.size();
    }
  }
}
