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
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.vfs.HopVfs;

public class ConfigFileSerializer implements IHopConfigSerializer {
  @Override
  public void writeToFile(String filename, Map<String, Object> configMap) throws HopException {
    try {
      ObjectMapper objectMapper = HopJson.newMapper();

      // Add option to indent arrays in the pretty printer
      DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
      prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);

      String niceJson = objectMapper.writer(prettyPrinter).writeValueAsString(configMap);
      byte[] content = niceJson.getBytes(StandardCharsets.UTF_8);

      Path localFile = localPath(filename);
      if (localFile == null) {
        writeThroughVfs(filename, content);
      } else {
        writeAtomically(localFile, content);
      }
    } catch (Exception e) {
      throw new HopException("Error writing to Hop configuration file : " + filename, e);
    }
  }

  /**
   * Write the configuration next to where it belongs and move it into place in one step.
   *
   * <p>The configuration used to be written to {@code <name>.new} and moved over the real file,
   * with the real file supposedly kept as {@code <name>.old} first. That never happened: {@code
   * canRenameTo} asks whether a rename is possible, it does not perform one, so there was no backup
   * - only a moment where the configuration existed under neither name. Two writers made it worse,
   * because both used those same two fixed names and tripped over each other's temporary file,
   * which is what Hop Web sessions saving at the same time ran into.
   *
   * <p>A temporary file of its own per write and a single move settle both: nobody shares a
   * temporary name, and a reader sees either the previous configuration or the new one.
   */
  private void writeAtomically(Path file, byte[] content) throws Exception {
    Path folder = file.toAbsolutePath().getParent();
    if (folder != null) {
      Files.createDirectories(folder);
    }
    // In the same folder as the file itself: a move is only atomic within one file store.
    Path temporary = Files.createTempFile(folder, file.getFileName().toString() + ".", ".tmp");
    try {
      Files.write(temporary, content);
      try {
        Files.move(
            temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (Exception atomicNotSupported) {
        // Not every file store can do it. Still better than writing the file in place.
        Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      // Only ever there when the move did not happen; leaving those behind would fill the
      // configuration folder with the leftovers of every failed save.
      Files.deleteIfExists(temporary);
    }
  }

  /** The same, for a configuration that does not live on this machine. */
  private void writeThroughVfs(String filename, byte[] content) throws Exception {
    FileObject temporary = HopVfs.getFileObject(filename + "." + UUID.randomUUID() + ".tmp");
    try {
      try (OutputStream outputStream = HopVfs.getOutputStream(temporary, false)) {
        outputStream.write(content);
      }
      HopVfs.moveFile(temporary, HopVfs.getFileObject(filename));
    } finally {
      if (temporary.exists()) {
        temporary.delete();
      }
    }
  }

  /**
   * The file as a local path, or null when it is somewhere only VFS can reach.
   *
   * <p>Decided on the name Hop was given rather than on what VFS makes of it: a location like
   * {@code s3://bucket/hop-config.json} is a perfectly valid relative path on Linux, and would
   * quietly be written to a folder called "s3:".
   */
  private static Path localPath(String filename) {
    if (filename == null || filename.contains("://")) {
      return null;
    }
    try {
      return Paths.get(filename);
    } catch (InvalidPathException e) {
      return null;
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
      ObjectMapper objectMapper = HopJson.newMapper();
      TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {};
      try (InputStream inputStream = HopVfs.getInputStream(file)) {
        return objectMapper.readValue(inputStream, typeRef);
      }
    } catch (Exception e) {
      throw new HopException("Error reading Hop configuration file " + filename, e);
    }
  }
}
