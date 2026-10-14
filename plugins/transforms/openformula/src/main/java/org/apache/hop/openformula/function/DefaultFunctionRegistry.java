/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.openformula.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.openformula.function.database.DatabaseFunctions;
import org.apache.hop.openformula.function.datetime.DateTimeFunctions;
import org.apache.hop.openformula.function.information.InformationFunctions;
import org.apache.hop.openformula.function.logical.LogicalFunctions;
import org.apache.hop.openformula.function.math.MathFunctions;
import org.apache.hop.openformula.function.rounding.RoundingFunctions;
import org.apache.hop.openformula.function.text.TextFunctions;
import org.apache.hop.openformula.function.userdefined.UserDefinedFunctions;

public class DefaultFunctionRegistry implements FunctionRegistry {
  private static final DefaultFunctionRegistry INSTANCE = new DefaultFunctionRegistry();

  private final Map<String, Function> functions = new HashMap<>();
  private final Map<String, FunctionDescription> metaData = new HashMap<>();
  private final Map<FunctionCategory, List<String>> categoryFunctions = new LinkedHashMap<>();

  public DefaultFunctionRegistry() {
    registerBuiltIns();
  }

  public static DefaultFunctionRegistry getInstance() {
    return INSTANCE;
  }

  private void registerBuiltIns() {
    LogicalFunctions.registerAll(this);
    RoundingFunctions.registerAll(this);
    DatabaseFunctions.registerAll(this);
    InformationFunctions.registerAll(this);
    MathFunctions.registerAll(this);
    DateTimeFunctions.registerAll(this);
    TextFunctions.registerAll(this);
    UserDefinedFunctions.registerAll(this);
  }

  public synchronized void registerFunction(Function function, FunctionDescription description) {
    String nameUpper = function.getCanonicalName().toUpperCase();
    functions.put(nameUpper, function);
    if (description != null) {
      metaData.put(nameUpper, description);
      FunctionCategory cat = description.getCategory();
      categoryFunctions
          .computeIfAbsent(cat, k -> new ArrayList<>())
          .add(function.getCanonicalName());
    }
  }

  @Override
  public Function findFunction(String name) {
    if (name == null) return null;
    return functions.get(name.toUpperCase());
  }

  @Override
  public FunctionDescription getMetaData(String name) {
    if (name == null) return null;
    return metaData.get(name.toUpperCase());
  }

  @Override
  public FunctionCategory[] getCategories() {
    return categoryFunctions.keySet().toArray(new FunctionCategory[0]);
  }

  @Override
  public String[] getFunctionNames() {
    return functions.keySet().toArray(new String[0]);
  }

  @Override
  public String[] getFunctionNamesByCategory(FunctionCategory category) {
    List<String> list = categoryFunctions.get(category);
    if (list == null) return new String[0];
    return list.toArray(new String[0]);
  }

  @Override
  public Function[] getFunctionsByCategory(FunctionCategory category) {
    List<String> names = categoryFunctions.get(category);
    if (names == null) return new Function[0];
    List<Function> result = new ArrayList<>(names.size());
    for (String name : names) {
      Function f = findFunction(name);
      if (f != null) result.add(f);
    }
    return result.toArray(new Function[0]);
  }
}
