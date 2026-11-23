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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * This metadata implementation is an in-memory metadata which serializes using JSON. In other
 * words, JSON is read into memory as a MetaStore and then you can ask to serialize that information
 * to and from JSON.
 */
public class SerializableMetadataProvider extends MemoryMetadataProvider
    implements IHopMetadataProvider {
  public SerializableMetadataProvider() {
    super();
  }

  /**
   * Create a copy of all elements in an existing metadata.
   *
   * <p>Objects which can't be loaded, typically because the plugin they reference isn't installed
   * (a Beam or Spark pipeline run configuration in a plain client, ...), are logged and skipped.
   * They shouldn't keep a pipeline or workflow which doesn't use them from running.
   *
   * @param source the source store to copy over
   */
  public SerializableMetadataProvider(IHopMetadataProvider source) throws HopException {
    this.description = "Serializable metadata provider (source is " + source.getDescription() + ")";

    // What is the list of available classes?
    //
    for (Class<IHopMetadata> metadataClass : source.getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> sourceSerializer = source.getSerializer(metadataClass);
      IHopMetadataSerializer<IHopMetadata> targetSerializer = getSerializer(metadataClass);

      // Loop over the available objects of the class and copy the information over.
      //
      for (String name : sourceSerializer.listObjectNames()) {
        try {
          IHopMetadata object = sourceSerializer.load(name);
          if (object != null) {
            targetSerializer.save(object);
          }
        } catch (Exception e) {
          logSkippedObject(name, metadataClass, e);
        }
      }
    }
  }

  private static void logSkippedObject(String name, Class<?> metadataClass, Exception e) {
    String message =
        "Error copying metadata object '"
            + name
            + "' of type "
            + metadataClass.getSimpleName()
            + ".  The object is ignored.";
    if (HopLogStore.isInitialized()) {
      LogChannel.GENERAL.logError(message, e);
    } else {
      // Metadata is also copied before any logging is available.
      //
      System.err.println(message + Const.CR + Const.getStackTracker(e));
    }
  }

  public String toJson() throws HopException {

    JSONObject jStore = new JSONObject();

    // What is the list of available classes?
    //
    for (Class<IHopMetadata> metadataClass : getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> serializer = getSerializer(metadataClass);
      HopMetadata hopMetadata = metadataClass.getAnnotation(HopMetadata.class);
      if (hopMetadata == null) {
        throw new HopException(
            "Error: class "
                + metadataClass
                + " is not annotated with "
                + HopMetadata.class.getName());
      }
      String classKey = hopMetadata.key();

      JSONArray jClass = new JSONArray();

      JsonMetadataParser parser = new JsonMetadataParser(metadataClass, this);

      // Loop over the available objects of the class and copy the information over to the JSON
      // store.
      // They are stored under plugin IDs...
      //
      for (String name : serializer.listObjectNames()) {
        Object object = serializer.load(name);
        JSONObject jObject = parser.getJsonObject((IHopMetadata) object);
        jClass.add(jObject);
      }

      jStore.put(classKey, jClass);
    }
    return jStore.toJSONString();
  }

  public SerializableMetadataProvider(String storeJson) throws HopException {
    this();
    this.description = "Serializable metadata provider (source is JSON)";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonFactory jsonFactory = objectMapper.getFactory();
    try {

      try (ByteArrayInputStream inputStream =
              new ByteArrayInputStream(storeJson.getBytes(StandardCharsets.UTF_8));
          JsonParser jsonParser = jsonFactory.createParser(inputStream)) {

        // Loop over the classes until there's no more left
        //
        jsonParser.nextToken(); // skip "{"
        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {

          String classKey = jsonParser.getText();
          if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new HopException("Expected a metadata array for key " + classKey);
          }

          // Snapshots include empty arrays for optional plugins which may not be installed here.
          // There is no metadata to load, so these types don't need to be resolved.
          //
          if (jsonParser.nextToken() == JsonToken.END_ARRAY) {
            continue;
          }
          Class<IHopMetadata> managedClass = getManagedClass(classKey);

          if (managedClass == null) {
            // Skip complete objects, including nested arrays, before advancing to the next type.
            //
            do {
              jsonParser.skipChildren();
            } while (jsonParser.nextToken() != JsonToken.END_ARRAY);
            continue;
          }

          JsonMetadataParser<IHopMetadata> metadataParser =
              new JsonMetadataParser<>(managedClass, this);

          IHopMetadataSerializer<IHopMetadata> serializer = getSerializer(managedClass);

          // Loop over the metadata objects in the JSON for the given class...
          //
          do {
            // One object at a time: an object that can't be built here, because it refers to a
            // plugin this Hop doesn't have, is left out instead of failing everything that reads
            // this metadata. It is read whole first, so the next object starts where it should.
            //
            JsonNode objectNode = objectMapper.readTree(jsonParser);
            try (JsonParser objectParser = objectNode.traverse(objectMapper)) {
              objectParser.nextToken();
              IHopMetadata object = metadataParser.loadJsonObject(managedClass, objectParser);
              if (object != null) {
                serializer.save(object);
              }
            } catch (Exception e) {
              logSkippedObject(classKey, objectNode, e);
            }
          } while (jsonParser.nextToken() != JsonToken.END_ARRAY);
        }
      }
    } catch (Exception e) {
      throw new HopException("Error reading metadata from JSON", e);
    }
  }

  /**
   * @param classKey
   * @return
   */
  private Class<IHopMetadata> getManagedClass(String classKey) {
    if (!isMetadataTypeInstalled(classKey)) {
      // Metadata written by a Hop with more plugins installed: nothing here can use it
      LogChannel.GENERAL.logBasic(
          "Skipping the metadata of type '"
              + classKey
              + "': no installed plugin provides this type.");
      return null;
    }
    Class<IHopMetadata> managedClass;
    try {
      managedClass = getMetadataClassForKey(classKey);
    } catch (HopException e) {
      LogChannel.GENERAL.logError("Error loading class " + classKey, e);
      managedClass = null;
    }
    return managedClass;
  }

  /** Whether an installed plugin provides this metadata type, under its key or a former key. */
  private boolean isMetadataTypeInstalled(String classKey) {
    if (PluginRegistry.getInstance().findPluginWithId(MetadataPluginType.class, classKey) != null) {
      return true;
    }
    for (Class<IHopMetadata> metadataClass : this.<IHopMetadata>getMetadataClasses()) {
      if (HopMetadataUtil.matchesKey(metadataClass.getAnnotation(HopMetadata.class), classKey)) {
        return true;
      }
    }
    return false;
  }

  /** One object of an installed type that this Hop can't build: one line, the cause included. */
  private static void logSkippedObject(String classKey, JsonNode objectNode, Exception e) {
    JsonNode name = objectNode.get("name");
    LogChannel.GENERAL.logError(
        "Skipping the metadata object '"
            + (name == null ? "?" : name.asText())
            + "' of type '"
            + classKey
            + "': it can't be loaded here. "
            + Const.NVL(e.getMessage(), e.toString()).trim());
    if (LogChannel.GENERAL.isDebug()) {
      LogChannel.GENERAL.logDebug(Const.getStackTracker(e));
    }
  }
}
