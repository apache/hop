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

package org.apache.hop.core.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;

/**
 * What a graph database type supports, read from the boolean capability methods of its {@link
 * IGraphDialect}: every public method without arguments returning a boolean and named {@code
 * isSupporting...}, {@code isRequiring...} or {@code isCypher}. The capability name is the method
 * name without {@code is}, starting with a lower case letter, for example {@code
 * supportingVectorIndexes}. Capabilities added to the dialect appear without changes here.
 */
@Getter
public class GraphDatabaseCapabilities {
  private static final Class<?> PKG = GraphDatabaseCapabilities.class;

  public static final String QUERY_LANGUAGE_CYPHER = "Cypher";
  public static final String QUERY_LANGUAGE_GREMLIN = "Gremlin";

  private static final String LABEL_KEY_PREFIX = "GraphDatabaseCapabilities.Capability.";

  private final String pluginId;
  private final String name;
  private final String description;
  private final String documentationUrl;
  private final String dialectId;
  private final String queryLanguage;

  /** The capabilities sorted by name. */
  private final Map<String, Boolean> capabilities;

  public GraphDatabaseCapabilities(
      String pluginId,
      String name,
      String description,
      String documentationUrl,
      IGraphDialect dialect) {
    this.pluginId = pluginId;
    this.name = name;
    this.description = description;
    this.documentationUrl = documentationUrl;
    this.dialectId = dialect == null ? null : dialect.getId();
    this.queryLanguage = getQueryLanguage(dialect);
    this.capabilities = Collections.unmodifiableMap(getCapabilities(dialect));
  }

  /**
   * The capabilities of a graph database type.
   *
   * @param pluginId The plugin ID of the graph database type, for example NEO4J
   * @throws HopException if there is no graph database type with that ID
   */
  public static GraphDatabaseCapabilities of(String pluginId) throws HopException {
    IPlugin plugin =
        PluginRegistry.getInstance().findPluginWithId(GraphDatabasePluginType.class, pluginId);
    if (plugin == null) {
      throw new HopException(
          "Unable to find graph database type plugin with ID '" + pluginId + "'");
    }
    return of(plugin, GraphDatabaseMeta.createGraphDatabase(pluginId));
  }

  /** The capabilities of the type of a graph database. */
  public static GraphDatabaseCapabilities of(IGraphDatabase graphDatabase) {
    IPlugin plugin = null;
    if (graphDatabase.getPluginId() != null) {
      plugin =
          PluginRegistry.getInstance()
              .findPluginWithId(GraphDatabasePluginType.class, graphDatabase.getPluginId());
    }
    return of(plugin, graphDatabase);
  }

  private static GraphDatabaseCapabilities of(IPlugin plugin, IGraphDatabase graphDatabase) {
    String id = graphDatabase.getPluginId();
    String typeName = graphDatabase.getPluginName();
    String typeDescription = null;
    String url = null;
    if (plugin != null) {
      id = plugin.getIds()[0];
      typeName = plugin.getName();
      typeDescription = plugin.getDescription();
      url = plugin.getDocumentationUrl();
    }
    return new GraphDatabaseCapabilities(
        id,
        typeName,
        StringUtils.trimToNull(typeDescription),
        StringUtils.trimToNull(url),
        graphDatabase.getGraphDialect());
  }

  /**
   * The capabilities of all installed graph database types, sorted by plugin ID.
   *
   * @throws HopException if a graph database type can't be created
   */
  public static List<GraphDatabaseCapabilities> getAll() throws HopException {
    List<GraphDatabaseCapabilities> all = new ArrayList<>();
    for (IPlugin plugin : PluginRegistry.getInstance().getPlugins(GraphDatabasePluginType.class)) {
      all.add(of(plugin.getIds()[0]));
    }
    all.sort((one, other) -> one.getPluginId().compareToIgnoreCase(other.getPluginId()));
    return all;
  }

  /**
   * Read the boolean capability methods of a dialect.
   *
   * @return The capabilities sorted by name, empty without a dialect
   */
  public static Map<String, Boolean> getCapabilities(IGraphDialect dialect) {
    Map<String, Boolean> map = new TreeMap<>();
    if (dialect == null) {
      return map;
    }
    for (Method method : dialect.getClass().getMethods()) {
      if (!isCapabilityMethod(method)) {
        continue;
      }
      Boolean value = invoke(dialect, method);
      if (value != null) {
        map.put(getCapabilityName(method.getName()), value);
      }
    }
    return map;
  }

  static boolean isCapabilityMethod(Method method) {
    if (Modifier.isStatic(method.getModifiers()) || method.getParameterCount() != 0) {
      return false;
    }
    Class<?> returnType = method.getReturnType();
    if (returnType != boolean.class && returnType != Boolean.class) {
      return false;
    }
    String methodName = method.getName();
    return "isCypher".equals(methodName)
        || isPrefixed(methodName, "isSupporting")
        || isPrefixed(methodName, "isRequiring");
  }

  private static boolean isPrefixed(String methodName, String prefix) {
    return methodName.length() > prefix.length() && methodName.startsWith(prefix);
  }

  private static Boolean invoke(IGraphDialect dialect, Method method) {
    // The method of a dialect class which isn't public, like an anonymous class, can't be
    // invoked through that class: invoke the interface method which it implements instead.
    //
    Method target = method;
    try {
      target = IGraphDialect.class.getMethod(method.getName());
    } catch (NoSuchMethodException e) {
      target.trySetAccessible();
    }
    try {
      return (Boolean) target.invoke(dialect);
    } catch (IllegalAccessException | InvocationTargetException | RuntimeException e) {
      // A dialect which can't answer without settings has no answer for the type
      return null;
    }
  }

  /**
   * @param methodName A capability method name, for example isSupportingVectorIndexes
   * @return The capability name, for example supportingVectorIndexes
   */
  public static String getCapabilityName(String methodName) {
    String withoutIs = methodName.startsWith("is") ? methodName.substring(2) : methodName;
    return StringUtils.uncapitalize(withoutIs);
  }

  /**
   * The label of a capability in the user interface, from the messages of this class, or derived
   * from the name for capabilities without one: supportingVectorSearch becomes "Vector search".
   */
  public static String getCapabilityLabel(String capability) {
    String label = BaseMessages.getString(PKG, LABEL_KEY_PREFIX + capability);
    if (label != null && !(label.startsWith("!") && label.endsWith("!"))) {
      return label;
    }
    String words = capability.startsWith("supporting") ? capability.substring(10) : capability;
    String[] parts = StringUtils.splitByCharacterTypeCamelCase(words);
    return StringUtils.capitalize(String.join(" ", parts).toLowerCase());
  }

  /** Cypher for the Cypher dialects, Gremlin for the others. */
  static String getQueryLanguage(IGraphDialect dialect) {
    if (dialect == null) {
      return null;
    }
    return dialect.isCypher() ? QUERY_LANGUAGE_CYPHER : QUERY_LANGUAGE_GREMLIN;
  }

  /** The fields in the order of the JSON output of hop-conf. */
  public Map<String, Object> toMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("pluginId", pluginId);
    map.put("name", name);
    map.put("description", description);
    map.put("documentationUrl", documentationUrl);
    map.put("dialectId", dialectId);
    map.put("queryLanguage", queryLanguage);
    map.put("capabilities", capabilities);
    return map;
  }

  /** Pretty printed JSON of maps, lists and simple values. */
  public static String toJson(Object value) throws HopException {
    try {
      return new ObjectMapper()
          .enable(SerializationFeature.INDENT_OUTPUT)
          .writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new HopException("Unable to write JSON", e);
    }
  }
}
