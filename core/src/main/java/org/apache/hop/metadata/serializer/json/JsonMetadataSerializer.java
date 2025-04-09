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

package org.apache.hop.metadata.serializer.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.json.simple.JSONObject;

/**
 * @param <T>
 */
public class JsonMetadataSerializer<T extends IHopMetadata> implements IHopMetadataSerializer<T> {

  @Setter @Getter protected IHopMetadataProvider metadataProvider;
  @Setter @Getter protected String baseFolder;
  @Setter @Getter protected Class<T> managedClass;
  @Setter @Getter protected String description;

  protected JsonMetadataParser<T> parser;
  protected IVariables variables;

  protected boolean baseFolderValidated;
  protected boolean baseFolderExists;

  public JsonMetadataSerializer(
      IHopMetadataProvider metadataProvider,
      String baseFolder,
      Class<T> managedClass,
      IVariables variables,
      String description) {
    this.metadataProvider = metadataProvider;
    this.baseFolder = baseFolder;
    this.managedClass = managedClass;
    this.parser = new JsonMetadataParser<>(managedClass, metadataProvider);
    this.variables = variables;
    this.description = description;
  }

  @Override
  public List<T> loadAll() throws HopException {
    List<T> list = new ArrayList<>();
    validateBaseFolder(false);
    if (!baseFolderExists) {
      return list;
    }
    List<String> names = listObjectNames();
    Collections.sort(names);
    for (String name : names) {
      list.add(load(name));
    }
    return list;
  }

  @Override
  public T load(String name) throws HopException {
    if (name == null) {
      throw new HopException("Error: you need to specify the name of the metadata object to load");
    }
    if (!exists(name)) {
      return null;
    }

    String filename = calculateFilename(name);

    try {
      // Load the JSON in a streaming fashion so we can parse the properties one by one...
      //
      try (InputStream fileInputStream = HopVfs.getInputStream(filename)) {
        JsonFactory jsonFactory = new JsonFactory();
        try (com.fasterxml.jackson.core.JsonParser jsonParser =
            jsonFactory.createParser(fileInputStream)) {

          // skip opening '{'
          jsonParser.nextToken();

          T t = parser.loadJsonObject(managedClass, jsonParser);
          inheritVariables(t);
          t.setMetadataProviderName(metadataProvider.getDescription());
          return t;
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error loading metadata object '" + name + "' from file '" + filename + "'", e);
    }
  }

  /**
   * If the loaded object implements variables we can inherit from it.
   *
   * @param t
   */
  private void inheritVariables(T t) {
    if (t instanceof IVariables iVariables) {
      iVariables.initializeFrom(variables);
    }
  }

  /**
   * If we want to save something with this serializer, a folder needs to exist for the type. If we
   * just want to read something, this is not required, and it's fine if the folder doesn't exist.
   *
   * @throws HopException In case we can't create a folder
   */
  protected void validateBaseFolder(boolean saveOperation) throws HopException {
    if (baseFolderValidated) {
      return;
    }

    // Check if the folder exists...
    //
    FileObject serializerBaseFolder = HopVfs.getFileObject(baseFolder);
    try {
      if (serializerBaseFolder.exists()) {
        baseFolderValidated = true;
        baseFolderExists = true;
      } else {
        if (saveOperation) {
          serializerBaseFolder.createFolder();
          baseFolderValidated = true;
          baseFolderExists = true;
        } else {
          // This read operation doesn't really require a folder to be created, but we haven't
          // validated
          // the base folder either.
          //
          baseFolderExists = false;
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error validating or creating folder  '"
              + baseFolder
              + "'to access JSON serialized objects from metadata class "
              + managedClass.getName(),
          e);
    }
  }

  @Override
  public void save(T t) throws HopException {
    if (StringUtils.isEmpty(t.getName())) {
      throw new HopException("Error: To save a metadata object it needs to have a name");
    }

    // Make sure the base folder exists
    //
    validateBaseFolder(true);

    String filename = calculateFilename(t.getName());
    try {

      JSONObject jObject = parser.getJsonObject(t);

      try (OutputStream outputStream = HopVfs.getOutputStream(filename, false)) {
        String jsonString = jObject.toJSONString();
        Gson gson = (new GsonBuilder()).setPrettyPrinting().create();
        JsonElement je = JsonParser.parseString(jsonString);

        String formattedJson = gson.toJson(je);
        outputStream.write(formattedJson.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        // Remember where we saved this...
        //
        t.setMetadataProviderName(getMetadataProvider().getDescription());
      } catch (IOException e) {
        throw new HopException("Error serializing JSON to file '" + filename + "'", e);
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to save object '" + t.getName() + "' to JSON file '" + filename + "'", e);
    }
  }

  public String calculateFilename(String name) {
    return baseFolder + "/" + name + ".json";
  }

  @Override
  public T delete(String name) throws HopException {
    // Make sure the base folder exists
    //
    validateBaseFolder(true);

    if (name == null) {
      throw new HopException(
          "Error: you need to specify the name of the metadata object to delete");
    }
    if (!exists(name)) {
      throw new HopException("Error: Object '" + name + "' doesn't exist");
    }
    T t = load(name);
    String filename = calculateFilename(name);
    try {
      boolean deleted = HopVfs.getFileObject(filename).delete();
      if (!deleted) {
        throw new HopException(
            "Error: Object '" + name + "' could not be deleted, filename : " + filename);
      }
    } catch (FileSystemException e) {
      throw new HopException("Error deleting Object '" + name + "' with filename : " + filename);
    }
    return t;
  }

  @Override
  public List<String> listObjectNames() throws HopException {
    List<String> names = new ArrayList<>();

    // Read-only access doesn't require a folder
    validateBaseFolder(false);
    if (!baseFolderExists) {
      // This is not an error.  We simply don't have objects of the given type.
      //
      return names;
    }

    FileObject folder = HopVfs.getFileObject(baseFolder);

    try {
      List<FileObject> jsonFiles = HopVfs.findFiles(folder, "json", false);
      for (FileObject jsonFile : jsonFiles) {
        String baseName = jsonFile.getName().getBaseName();
        names.add(baseName.replaceAll("\\.json$", ""));
      }
      return names;
    } catch (Exception e) {
      throw new HopException("Error searching for JSON files", e);
    }
  }

  @Override
  public boolean exists(String name) throws HopException {
    // Read-only access doesn't require a folder
    validateBaseFolder(false);
    if (!baseFolderExists) {
      return false;
    }
    return HopVfs.fileExists(calculateFilename(name));
  }
}
