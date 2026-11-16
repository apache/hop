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

package org.apache.hop.core.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataDefaultObjectFactory;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * An object that a metadata property creates through an object factory (an execution information
 * location, a sampler, a run configuration, ...) is saved under its ID, and created again from that
 * ID when it is read. Read and saved again, as in a metadata export or the metadata that remote and
 * distributed engines send their workers, every registered plugin must come back as itself.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class MetadataObjectIdTest {

  /** As the location files of the Neo4j integration tests: the plugin ID is only the key. */
  @Test
  void aLocationStoredOnlyUnderItsKeyKeepsItsTypeWhenSavedAgain() throws Exception {
    String key = ExecutionInfoLocation.class.getAnnotation(HopMetadata.class).key();
    String exported =
        "{\""
            + key
            + "\":[{\"name\":\"audit\",\"dataLoggingDelay\":\"2000\",\"dataLoggingInterval\":\"5000\","
            + "\"executionInfoLocation\":{\"local-folder\":{\"rootFolder\":\"/tmp/executions\"}}}]}";

    String exportedAgain = new SerializableMetadataProvider(exported).toJson();
    ExecutionInfoLocation location =
        new SerializableMetadataProvider(exportedAgain)
            .getSerializer(ExecutionInfoLocation.class)
            .load("audit");

    FileExecutionInfoLocation fileLocation =
        assertInstanceOf(FileExecutionInfoLocation.class, location.getExecutionInfoLocation());
    assertEquals("local-folder", fileLocation.getPluginId());
    assertEquals("/tmp/executions", fileLocation.getRootFolder());
  }

  @Test
  void everyRegisteredPluginComesBackAsItselfWhenSavedAgain() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<String> failures = new ArrayList<>();
    Map<String, Integer> roundTrips = new TreeMap<>();

    for (IPlugin metadataPlugin : registry.getPlugins(MetadataPluginType.class)) {
      Class<?> holderClass = registry.loadClass(metadataPlugin).getClass();
      for (Field field : factoryFields(holderClass)) {
        Class<?> valueType = valueType(field);
        IHopMetadataObjectFactory factory =
            valueType
                .getAnnotation(HopMetadataObject.class)
                .objectFactory()
                .getDeclaredConstructor()
                .newInstance();
        String property = holderClass.getSimpleName() + "." + field.getName();
        for (String id : pluginIds(registry, valueType)) {
          // As read from a file, and as the copy an editor or a run works on
          Object read = factory.createObject(id, null);
          List<Object> objects = new ArrayList<>(List.of(read));
          Method clone = publicClone(read);
          if (clone != null) {
            objects.add(clone.invoke(read));
          }
          for (Object object : objects) {
            roundTrips.merge(property, 1, Integer::sum);
            String problem = roundTrip(holderClass, field, object);
            if (problem != null) {
              failures.add(property + ", " + id + ": " + problem);
            }
          }
        }
      }
    }

    assertTrue(failures.isEmpty(), String.join("\n", failures));
    // The plugins of the engine module itself are covered
    for (String property :
        List.of(
            "ExecutionInfoLocation.executionInfoLocation",
            "ExecutionDataProfile.samplers",
            "PipelineRunConfiguration.engineRunConfiguration",
            "WorkflowRunConfiguration.engineRunConfiguration")) {
      assertTrue(roundTrips.getOrDefault(property, 0) > 0, property + " not tested: " + roundTrips);
    }
  }

  /** The properties holding an object that a factory of its own saves and creates. */
  private static List<Field> factoryFields(Class<?> holderClass) {
    List<Field> fields = new ArrayList<>();
    for (Class<?> c = holderClass; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (field.getAnnotation(HopMetadataProperty.class) == null) {
          continue;
        }
        Class<?> valueType = valueType(field);
        HopMetadataObject annotation =
            valueType == null ? null : valueType.getAnnotation(HopMetadataObject.class);
        if (annotation != null
            && annotation.objectFactory() != HopMetadataDefaultObjectFactory.class) {
          fields.add(field);
        }
      }
    }
    return fields;
  }

  /** The type of the field, or of the elements of a list. */
  private static Class<?> valueType(Field field) {
    if (!List.class.isAssignableFrom(field.getType())) {
      return field.getType();
    }
    if (field.getGenericType() instanceof ParameterizedType type
        && type.getActualTypeArguments()[0] instanceof Class<?> elementType) {
      return elementType;
    }
    return null;
  }

  /** The IDs of the registered plugins the factory creates objects of the type for. */
  private static List<String> pluginIds(PluginRegistry registry, Class<?> valueType) {
    List<String> ids = new ArrayList<>();
    for (var pluginType : registry.getPluginTypes()) {
      boolean matches;
      if (valueType == IPipelineEngineRunConfiguration.class) {
        matches = pluginType == PipelineEnginePluginType.class;
      } else if (valueType == IWorkflowEngineRunConfiguration.class) {
        matches = pluginType == WorkflowEnginePluginType.class;
      } else if (valueType == IDatabase.class) {
        // The database plugin type doesn't name its main type
        matches = pluginType == DatabasePluginType.class;
      } else {
        matches = false;
      }
      for (IPlugin plugin : registry.getPlugins(pluginType)) {
        if (matches || plugin.getMainType() == valueType) {
          ids.add(plugin.getIds()[0]);
        }
      }
    }
    return ids;
  }

  private static Method publicClone(Object object) {
    try {
      return object.getClass().getMethod("clone");
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /** Null when the object is read back as itself from the JSON it was saved in. */
  @SuppressWarnings("unchecked")
  private static String roundTrip(Class<?> holderClass, Field field, Object object) {
    try {
      IHopMetadata holder = (IHopMetadata) holderClass.getDeclaredConstructor().newInstance();
      holder.setName("probe");
      field.setAccessible(true);
      boolean list = List.class.isAssignableFrom(field.getType());
      field.set(holder, list ? new ArrayList<>(List.of(object)) : object);

      SerializableMetadataProvider saved = new SerializableMetadataProvider();
      ((IHopMetadataSerializer<IHopMetadata>)
              saved.getSerializer((Class<IHopMetadata>) holderClass))
          .save(holder);
      SerializableMetadataProvider read = new SerializableMetadataProvider(saved.toJson());
      Object loaded = read.getSerializer((Class<IHopMetadata>) holderClass).load("probe");

      Object value = loaded == null ? null : field.get(loaded);
      if (list && value != null) {
        value = ((List<?>) value).isEmpty() ? null : ((List<?>) value).get(0);
      }
      if (value == null) {
        return "not read back";
      }
      return value.getClass() == object.getClass()
          ? null
          : "read back as " + value.getClass().getName();
    } catch (Exception e) {
      return e.toString();
    }
  }
}
