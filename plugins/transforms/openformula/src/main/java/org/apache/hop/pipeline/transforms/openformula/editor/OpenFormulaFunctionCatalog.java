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

package org.apache.hop.pipeline.transforms.openformula.editor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.openformula.DefaultFormulaContext;
import org.apache.hop.openformula.function.FunctionCategory;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.FunctionRegistry;

/** Function tree for the formula editor, sourced from hop-formula's registry. */
public class OpenFormulaFunctionCatalog {
  private final Map<String, List<FunctionDescription>> byCategory = new LinkedHashMap<>();

  public OpenFormulaFunctionCatalog() {
    FunctionRegistry registry = new DefaultFormulaContext().getFunctionRegistry();
    FunctionCategory[] categories = registry.getCategories();
    Arrays.sort(categories, Comparator.comparing(c -> c.getDisplayName(Locale.getDefault())));
    for (FunctionCategory category : categories) {
      String[] names = registry.getFunctionNamesByCategory(category);
      Arrays.sort(names, String.CASE_INSENSITIVE_ORDER);
      List<FunctionDescription> functions = new ArrayList<>();
      for (String name : names) {
        FunctionDescription meta = registry.getMetaData(name);
        if (meta != null) {
          functions.add(meta);
        }
      }
      if (!functions.isEmpty()) {
        byCategory.put(category.getDisplayName(Locale.getDefault()), functions);
      }
    }
  }

  public String[] getCategories() {
    return byCategory.keySet().toArray(String[]::new);
  }

  public List<FunctionDescription> getFunctions(String category) {
    return byCategory.getOrDefault(category, List.of());
  }

  public static String syntax(FunctionDescription description) {
    StringBuilder builder = new StringBuilder(description.getCanonicalName());
    builder.append('(');
    int count = description.getParameterCount();
    Locale locale = Locale.getDefault();
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        builder.append(';');
      }
      builder.append(description.getParameterDisplayName(i, locale));
    }
    if (description.isInfiniteParameterCount()) {
      if (count > 0) {
        builder.append(';');
      }
      builder.append("...");
    }
    builder.append(')');
    return builder.toString();
  }

  public static String htmlReport(FunctionDescription description) {
    Locale locale = Locale.getDefault();
    StringBuilder html = new StringBuilder();
    html.append("<html><body style='font-family:sans-serif;font-size:12px'>");
    html.append("<h3>").append(escape(description.getCanonicalName())).append("</h3>");
    html.append("<p><b>").append(escape(syntax(description))).append("</b></p>");
    String desc = description.getDescription(locale);
    if (desc != null && !desc.isBlank()) {
      html.append("<p>").append(escape(desc)).append("</p>");
    }
    int count = description.getParameterCount();
    if (count > 0) {
      html.append("<ul>");
      for (int i = 0; i < count; i++) {
        html.append("<li><b>")
            .append(escape(description.getParameterDisplayName(i, locale)))
            .append("</b>: ")
            .append(escape(nullToEmpty(description.getParameterDescription(i, locale))))
            .append("</li>");
      }
      html.append("</ul>");
    }
    html.append("</body></html>");
    return html.toString();
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static String escape(String value) {
    if (value == null) {
      return "";
    }
    return value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;");
  }
}
