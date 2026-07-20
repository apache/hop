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

package org.apache.hop.ui.core.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.DescribedVariable;

/**
 * Builds proposed environment variables for a named relational database connection (for example
 * {@code EDW_HOSTNAME}) and maps dialog results back to connection fields by name suffix.
 */
public final class DatabaseConnectionVariablesHelper {

  public static final String SUFFIX_HOSTNAME = "_HOSTNAME";
  public static final String SUFFIX_PORT = "_PORT";
  public static final String SUFFIX_DATABASE = "_DATABASE";
  public static final String SUFFIX_USERNAME = "_USERNAME";
  public static final String SUFFIX_PASSWORD = "_PASSWORD";
  public static final String SUFFIX_URL = "_URL";

  private static final String[] FIELD_SUFFIXES = {
    SUFFIX_HOSTNAME, SUFFIX_PORT, SUFFIX_DATABASE, SUFFIX_USERNAME, SUFFIX_PASSWORD, SUFFIX_URL,
  };

  private DatabaseConnectionVariablesHelper() {
    // Utility class
  }

  /**
   * Sanitize a connection name into a variable name prefix.
   *
   * <p>Example: {@code "My DB"} → {@code "MY_DB"}, {@code "EDW"} → {@code "EDW"}.
   *
   * @param name connection name
   * @return uppercase prefix safe for variable names, or empty string if name is blank
   */
  public static String sanitizeConnectionNamePrefix(String name) {
    if (StringUtils.isBlank(name)) {
      return "";
    }
    String prefix = name.trim().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9_]+", "_");
    prefix = prefix.replaceAll("_+", "_");
    return StringUtils.strip(prefix, "_");
  }

  /**
   * @param varName variable name without braces
   * @return Hop variable expression such as {@code ${EDW_HOSTNAME}}
   */
  public static String expressionFor(String varName) {
    return "${" + varName + "}";
  }

  /**
   * Build proposed described variables from the current connection field values.
   *
   * @param connectionName human-readable connection name (used in descriptions)
   * @param hostname current hostname value
   * @param port current port value
   * @param databaseName current database name value
   * @param username current username value
   * @param password current password value
   * @param manualUrl current manual URL value (variable proposed only when non-blank)
   * @param includeUsername whether username is part of the editor
   * @param includePassword whether password is part of the editor
   * @param descriptionsBySuffix map of suffix → description text (e.g. {@code _HOSTNAME} → "…")
   * @return ordered list of proposed variables
   */
  public static List<DescribedVariable> buildProposedVariables(
      String connectionName,
      String hostname,
      String port,
      String databaseName,
      String username,
      String password,
      String manualUrl,
      boolean includeUsername,
      boolean includePassword,
      Map<String, String> descriptionsBySuffix) {
    String prefix = sanitizeConnectionNamePrefix(connectionName);
    List<DescribedVariable> variables = new ArrayList<>();
    if (StringUtils.isEmpty(prefix)) {
      return variables;
    }

    addVariable(variables, prefix, SUFFIX_HOSTNAME, hostname, descriptionsBySuffix);
    addVariable(variables, prefix, SUFFIX_PORT, port, descriptionsBySuffix);
    addVariable(variables, prefix, SUFFIX_DATABASE, databaseName, descriptionsBySuffix);

    if (includeUsername) {
      addVariable(variables, prefix, SUFFIX_USERNAME, username, descriptionsBySuffix);
    }
    if (includePassword) {
      addVariable(variables, prefix, SUFFIX_PASSWORD, password, descriptionsBySuffix);
    }
    if (StringUtils.isNotBlank(manualUrl)) {
      addVariable(variables, prefix, SUFFIX_URL, manualUrl, descriptionsBySuffix);
    }

    return variables;
  }

  /**
   * Map variable names from a (possibly edited) list by known field suffix. If multiple names share
   * a suffix, the first match wins (stable order of the list).
   *
   * @param variables variables returned from the review dialog
   * @return suffix → full variable name
   */
  public static Map<String, String> findVariableNamesBySuffix(List<DescribedVariable> variables) {
    Map<String, String> bySuffix = new LinkedHashMap<>();
    if (variables == null) {
      return bySuffix;
    }
    for (DescribedVariable variable : variables) {
      if (variable == null || StringUtils.isBlank(variable.getName())) {
        continue;
      }
      String name = variable.getName().trim();
      for (String suffix : FIELD_SUFFIXES) {
        if (name.endsWith(suffix) && !bySuffix.containsKey(suffix)) {
          bySuffix.put(suffix, name);
          break;
        }
      }
    }
    return bySuffix;
  }

  /**
   * Suggest a default config filename for the connection prefix (not path-resolved).
   *
   * @param prefix sanitized connection prefix
   * @return e.g. {@code EDW-config.json}
   */
  public static String defaultConfigFilename(String prefix) {
    if (StringUtils.isEmpty(prefix)) {
      return "database-config.json";
    }
    return prefix + "-config.json";
  }

  private static void addVariable(
      List<DescribedVariable> variables,
      String prefix,
      String suffix,
      String value,
      Map<String, String> descriptionsBySuffix) {
    String name = prefix + suffix;
    String description =
        descriptionsBySuffix != null ? Const.NVL(descriptionsBySuffix.get(suffix), "") : "";
    variables.add(new DescribedVariable(name, Const.NVL(value, ""), description));
  }
}
