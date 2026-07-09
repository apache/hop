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

package org.apache.hop.pipeline.transforms.yamlinput;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/** Normalizes and resolves YAML/JSON-style path expressions for JsonPath evaluation. */
public final class YamlPathResolver {
  public static final String DOLLAR = "$";

  private YamlPathResolver() {}

  /**
   * Turns a legacy flat key or dotted path into a JsonPath expression. Paths that already start
   * with {@code $} are returned unchanged.
   */
  public static String normalize(String path) {
    if (Utils.isEmpty(path)) {
      return DOLLAR;
    }

    if (path.startsWith(DOLLAR)) {
      return path;
    }
    return "$." + path;
  }

  /**
   * Substitutes pipeline variables in a path without mangling JsonPath syntax such as {@code $[*]}.
   */
  public static String resolvePath(IVariables variables, String path) {
    if (Utils.isEmpty(path) || variables == null) {
      return normalize(path);
    }

    Map<String, String> variableMap = new HashMap<>();
    for (String name : variables.getVariableNames()) {
      variableMap.put(name, variables.getVariable(name));
    }

    String substituted =
        StringUtil.substituteUnix(StringUtil.substituteWindows(path, variableMap), variableMap);
    return normalize(substituted);
  }
}
