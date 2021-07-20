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

package org.apache.hop.core.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ConfigFileSerializer implements IHopConfigSerializer {
  @Override
  public void writeToFile(String filename, Map<String, Object> configMap) throws HopException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      String niceJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(configMap);

      // Write to a new new file...
      //
      FileObject newFile = HopVfs.getFileObject(filename + ".new");
      if (newFile.exists()) {
        if (!newFile.delete()) {
          throw new HopException("Unable to delete new config file " + newFile.getName().getURI());
        }
      }

      // Write to the new file (hop.config.new)
      //
      OutputStream outputStream = HopVfs.getOutputStream(newFile, false);
      outputStream.write(niceJson.getBytes(StandardCharsets.UTF_8));
      outputStream.close();

      // if this worked, delete the old file  (hop.config.old)
      //
      FileObject oldFile = HopVfs.getFileObject(filename + ".old");
      if (oldFile.exists()) {
        if (!oldFile.delete()) {
          throw new HopException("Unable to delete old config file " + oldFile.getName().getURI());
        }
      }

      // If this worked, rename the file to the old file  (hop.config -> hop.config.old)
      //
      FileObject file = HopVfs.getFileObject(filename);
      if (file.exists()) { // could be a new file
        if (!file.canRenameTo(oldFile)) {
          throw new HopException(
              "Unable to rename config file to .old : " + file.getName().getURI());
        }
      }

      // Now rename the new file to the final value...
      //
      newFile.moveTo(file);
    } catch (Exception e) {
      throw new HopException("Error writing to Hop configuration file : " + filename, e);
    }
  }

  @Override
  public Map<String, Object> readFromFile(String filename) throws HopException {
    try {
      FileObject file = HopVfs.getFileObject(filename);
      if (!file.exists()) {
        // Just an empty config map.
        //
        return new HashMap<>();
      }
      ObjectMapper objectMapper = new ObjectMapper();
      TypeReference<HashMap<String, Object>> typeRef =
          new TypeReference<HashMap<String, Object>>() {};
      try (InputStream inputStream = HopVfs.getInputStream(file)) {
        HashMap<String, Object> configMap = objectMapper.readValue(inputStream, typeRef);
        return configMap;
      }
    } catch (Exception e) {
      throw new HopException("Error reading Hop configuration file " + filename, e);
    }
  }
}
