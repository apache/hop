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
package org.apache.hop.lint;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * Loads Hop metadata objects from project metadata JSON files.
 *
 * <p>Hop stores metadata as {@code metadata/<key>/<name>.json}, where {@code <key>} is the metadata
 * type's registered key, the file name is the object's name, and the document nests the payload
 * under the type key and then the plugin id, for example {@code {"rdbms": {"POSTGRESQL": {...}}}}.
 *
 * <p>Any registered metadata type can be loaded, not a hard-coded list: the folder name is the key
 * the plugin registry indexes types by, so {@code getMetadataClassForKey} turns the path into the
 * managed class and Hop's own serializer does the rest. Third-party metadata types work without
 * this class knowing about them.
 */
public final class HopMetadataFileLoader {

  private static final ILogChannel log = LogChannel.GENERAL;

  private HopMetadataFileLoader() {}

  public static boolean isMetadataJsonFile(String path) {
    if (Utils.isEmpty(path)) {
      return false;
    }
    // Normalise separators first: on Windows these paths arrive with backslashes, and matching
    // only "/metadata/" made every metadata rule silently no-op there.
    String lower = path.toLowerCase().replace('\\', '/');
    return lower.contains("/metadata/") && lower.endsWith(".json");
  }

  /**
   * The metadata type key for a file, taken from the folder directly under {@code metadata/}.
   *
   * @return the key, e.g. "rdbms" or "pipeline-run-configuration", or null when the path is not
   *     laid out as Hop metadata
   */
  public static String metadataKeyOf(String path) {
    if (Utils.isEmpty(path)) {
      return null;
    }
    String normalised = path.replace('\\', '/');
    int marker = normalised.toLowerCase().lastIndexOf("/metadata/");
    if (marker < 0) {
      return null;
    }
    String rest = normalised.substring(marker + "/metadata/".length());
    int slash = rest.indexOf('/');
    // Needs a folder between metadata/ and the file: metadata/foo.json has no type.
    return slash > 0 ? rest.substring(0, slash) : null;
  }

  /** The object's name is its file name without the extension, as Hop's own serializers do. */
  public static String metadataNameOf(File file) {
    String name = file.getName();
    int dot = name.lastIndexOf('.');
    return dot > 0 ? name.substring(0, dot) : name;
  }

  /**
   * The outcome of trying to read a metadata file.
   *
   * @param object the loaded object, null when it could not be read
   * @param error why it could not be read, null on success and null when the type simply is not
   *     installed — a project may legitimately carry metadata for a plugin this Hop does not have
   */
  public record MetadataLoad(Object object, String error) {

    static MetadataLoad of(Object object) {
      return new MetadataLoad(object, null);
    }

    static MetadataLoad skipped() {
      return new MetadataLoad(null, null);
    }

    static MetadataLoad failed(String error) {
      return new MetadataLoad(null, error);
    }

    public boolean isFailure() {
      return object == null && error != null;
    }
  }

  public static Object loadMetadataObject(File file) {
    return loadMetadataObject(file, null);
  }

  public static Object loadMetadataObject(File file, IHopMetadataProvider metadataProvider) {
    return read(file, metadataProvider).object();
  }

  /**
   * Load a metadata object from disk.
   *
   * <p>With a metadata provider the object is built exactly as Hop would build it — plugin
   * attributes, password decoding and all. Without one only relational connections can be read, by
   * unwrapping the document by hand, because every other type needs its plugin class resolved
   * before the payload means anything.
   */
  /**
   * Read a metadata file, distinguishing "cannot be read" from "not applicable".
   *
   * <p>A file that fails to parse is reported rather than skipped. Skipping it silently would mean
   * a corrupt or hand-edited connection passes every rule and the run says the project is clean,
   * which is the one outcome a linter must never produce.
   */
  public static MetadataLoad read(File file, IHopMetadataProvider metadataProvider) {
    if (file == null || !file.isFile()) {
      return MetadataLoad.skipped();
    }

    String path = file.getAbsolutePath();
    if (!isMetadataJsonFile(path)) {
      return MetadataLoad.skipped();
    }

    String key = metadataKeyOf(path);
    if (key == null) {
      log.logDetailed("Metadata file is not inside a type folder, skipping: " + path);
      return MetadataLoad.skipped();
    }

    String name = metadataNameOf(file);

    if (metadataProvider != null) {
      Class<IHopMetadata> managedClass = managedClassFor(metadataProvider, key);
      if (managedClass == null) {
        // The type is not installed. Not a finding: a project may carry metadata for a
        // plugin this Hop does not have.
        log.logDetailed("No metadata plugin registered for type '" + key + "', skipping " + path);
        return MetadataLoad.skipped();
      }
      try {
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(managedClass);
        IHopMetadata loaded = serializer.load(name);
        if (loaded != null) {
          return MetadataLoad.of(named(loaded, name));
        }
        return MetadataLoad.failed("the metadata provider returned nothing for '" + name + "'");
      } catch (Exception e) {
        return MetadataLoad.failed(rootMessage(e));
      }
    }

    // Best effort without a provider. Only relational connections can be reconstructed by hand;
    // for anything else the plugin class is what gives the document meaning.
    if ("rdbms".equalsIgnoreCase(key)) {
      DatabaseMeta databaseMeta = readDatabaseMetaDirectly(file);
      return databaseMeta != null
          ? MetadataLoad.of(named(databaseMeta, name))
          : MetadataLoad.failed("could not be parsed as a database connection");
    }

    log.logDetailed(
        "Metadata type '" + key + "' needs a metadata provider to be linted; skipping " + path);
    return MetadataLoad.skipped();
  }

  /** The managed class for a metadata key, or null when no plugin registers that key. */
  private static Class<IHopMetadata> managedClassFor(
      IHopMetadataProvider metadataProvider, String key) {
    try {
      return metadataProvider.getMetadataClassForKey(key);
    } catch (Exception e) {
      return null;
    }
  }

  /** Hop nests its causes deeply; the innermost message is the one that says what is wrong. */
  private static String rootMessage(Throwable t) {
    Throwable root = t;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String message = root.getMessage();
    if (Utils.isEmpty(message)) {
      message = root.getClass().getSimpleName();
    }
    return message.trim().replaceAll("\\s*\\R\\s*", " ");
  }

  /**
   * Parse {@code metadata/rdbms/<name>.json} without a provider by unwrapping the type and
   * plugin-id envelopes, then handing the inner object to Hop's own mapper.
   */
  private static DatabaseMeta readDatabaseMetaDirectly(File file) {
    try {
      String json = Files.readString(file.toPath(), StandardCharsets.UTF_8);
      JsonNode root = HopJson.newMapper().readTree(json);

      JsonNode payload = unwrap(unwrap(root, "rdbms"), null);
      if (payload == null || !payload.isObject()) {
        log.logDetailed("Unrecognised metadata layout, skipping: " + file.getAbsolutePath());
        return null;
      }

      return HopJson.newMapper().treeToValue(payload, DatabaseMeta.class);
    } catch (Exception e) {
      log.logError(
          "Failed to load metadata file " + file.getAbsolutePath() + ": " + e.getMessage(), e);
      return null;
    }
  }

  /**
   * Stamp the object's name on if the document did not carry one.
   *
   * <p>The name lives in the file name, not the document, and Hop's serializers leave it blank on
   * load. Rules targeting the name — "no environment marker in the connection name", naming
   * conventions — would see an empty string and silently pass without this.
   */
  private static Object named(Object metadataObject, String name) {
    if (metadataObject == null) {
      return null;
    }
    try {
      Method getName = metadataObject.getClass().getMethod("getName");
      Object current = getName.invoke(metadataObject);
      if (current == null || Utils.isEmpty(current.toString())) {
        Method setName = metadataObject.getClass().getMethod("setName", String.class);
        setName.invoke(metadataObject, name);
      }
    } catch (NoSuchMethodException e) {
      // A metadata type without a name property is unusual but not an error.
      log.logRowlevel("Metadata object has no name property: " + metadataObject.getClass());
    } catch (Exception e) {
      log.logDetailed("Could not set the name on a metadata object: " + e.getMessage());
    }
    return metadataObject;
  }

  /**
   * Step one level into the document. With a key, take that field if present; without one, take the
   * single child object, which is how the plugin-id level is keyed.
   */
  private static JsonNode unwrap(JsonNode node, String key) {
    if (node == null || !node.isObject()) {
      return node;
    }
    if (key != null) {
      return node.has(key) ? node.get(key) : node;
    }
    if (node.size() == 1) {
      Iterator<JsonNode> children = node.elements();
      JsonNode only = children.next();
      // Only descend when the single child is itself an object, so a one-field connection
      // definition is not mistaken for an envelope.
      if (only.isObject() && only.size() > 1) {
        return only;
      }
    }
    return node;
  }
}
