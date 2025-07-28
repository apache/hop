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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;

public class FunctionLib {

  private List<FunctionDescription> functions;

  public FunctionLib() throws HopException {
    functions = new ArrayList<>();
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.getPlugin(TransformPluginType.class, "Janino");
      ClassLoader loader = registry.getClassLoader(plugin);
      Set<Class<?>> classes =
          findAllClassesUsingGoogleGuice(loader, "org.apache.hop.pipeline.transforms.janino");

      for (Class<?> clazz : classes) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
          JaninoFunction annotation = method.getAnnotation(JaninoFunction.class);
          List<FunctionExample> functionExamples = new ArrayList<>();
          if (annotation != null) {
            if (!Utils.isEmpty(annotation.examples())) {
              ObjectMapper mapper = new ObjectMapper();
              JsonNode arrayNode = mapper.readTree(annotation.examples());
              for (JsonNode jsonNode : arrayNode) {
                functionExamples.add(
                    new FunctionExample(
                        jsonNode.get("expression").asText(),
                        jsonNode.get("result").asText(),
                        jsonNode.get("level").asText(),
                        jsonNode.get("comment").asText()));
              }
            }

            FunctionDescription functionDescription =
                new FunctionDescription(
                    annotation.category(),
                    annotation.name(),
                    annotation.description(),
                    annotation.syntax(),
                    annotation.returns(),
                    null,
                    annotation.semantics(),
                    clazz.getCanonicalName(),
                    functionExamples);
            functions.add(functionDescription);
          }
        }
      }

    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * @return the functions
   */
  public List<FunctionDescription> getFunctions() {
    return functions;
  }

  /**
   * @param functions the functions to set
   */
  public void setFunctions(List<FunctionDescription> functions) {
    this.functions = functions;
  }

  /**
   * @return A sorted array of function names, extracted from the function descriptions...
   */
  public String[] getFunctionNames() {
    String[] names = new String[functions.size()];
    for (int i = 0; i < functions.size(); i++) {
      names[i] = functions.get(i).getName();
    }
    Arrays.sort(names);
    return names;
  }

  /**
   * @return A sorted array of function names, extracted from the function descriptions...
   */
  public String[] getImportPackages() {
    ArrayList<String> importPackages = new ArrayList<>();
    for (int i = 0; i < functions.size(); i++) {
      importPackages.add(functions.get(i).getImportPackage());
    }
    importPackages.sort(Comparator.naturalOrder());
    return importPackages.stream().distinct().toArray(String[]::new);
  }

  /**
   * @return A sorted array of unique categories, extracted from the function descriptions...
   */
  public String[] getFunctionCategories() {
    List<String> categories = new ArrayList<>();
    for (FunctionDescription function : functions) {
      String category = function.getCategory();
      if (!categories.contains(category)) {
        categories.add(category);
      }
    }
    Collections.sort(categories);
    return categories.toArray(new String[categories.size()]);
  }

  /**
   * Get all the function names for a certain category
   *
   * @param category the category name to look for
   * @return the sorted array of function names for the specified category
   */
  public String[] getFunctionsForACategory(String category) {
    List<String> names = new ArrayList<>();
    for (FunctionDescription function : functions) {
      if (function.getCategory().equalsIgnoreCase(category)) {
        names.add(function.getName());
      }
    }
    Collections.sort(names);
    return names.toArray(new String[names.size()]);
  }

  /**
   * @param functionName the name of the function to look for
   * @return the corresponding function description or null if nothing was found.
   */
  public FunctionDescription getFunctionDescription(String functionName) {
    for (FunctionDescription function : functions) {
      if (function.getName().equalsIgnoreCase(functionName)) {
        return function;
      }
    }
    return null;
  }

  public Set<Class<?>> findAllClassesUsingGoogleGuice(ClassLoader classLoader, String packageName)
      throws IOException {
    return ClassPath.from(classLoader).getAllClasses().stream()
        .filter(clazz -> clazz.getPackageName().contains(packageName))
        .map(ClassPath.ClassInfo::load)
        .collect(Collectors.toSet());
  }
}
