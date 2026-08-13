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

package org.apache.hop.setup.persist;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.setup.HopSetupException;

/** HopVfs helpers for the setup writers. */
public final class HopVfsFiles {

  private HopVfsFiles() {}

  public static boolean exists(String path) throws HopSetupException {
    try {
      return HopVfs.fileExists(path);
    } catch (Exception e) {
      throw new HopSetupException("Unable to check file '" + path + "'", e);
    }
  }

  public static String readUtf8(String path) throws HopSetupException {
    try (InputStream in = HopVfs.getInputStream(path)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new HopSetupException("Unable to read '" + path + "'", e);
    }
  }

  public static void writeUtf8(String path, String content) throws HopSetupException {
    try {
      FileObject file = HopVfs.getFileObject(path);
      FileObject parent = file.getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
      try (OutputStream out = HopVfs.getOutputStream(file, false)) {
        out.write(content.getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      throw new HopSetupException("Unable to write '" + path + "'", e);
    }
  }

  public static void createFolder(String path) throws HopSetupException {
    try {
      FileObject folder = HopVfs.getFileObject(path);
      if (!folder.exists()) {
        folder.createFolder();
      }
    } catch (Exception e) {
      throw new HopSetupException("Unable to create folder '" + path + "'", e);
    }
  }

  public static void copyTree(String sourcePath, String destinationPath) throws HopSetupException {
    try {
      FileObject source = HopVfs.getFileObject(sourcePath);
      FileObject destination = HopVfs.getFileObject(destinationPath);
      if (!destination.exists()) {
        destination.createFolder();
      }
      destination.copyFrom(source, Selectors.SELECT_CHILDREN);
    } catch (Exception e) {
      throw new HopSetupException(
          "Unable to copy '" + sourcePath + "' to '" + destinationPath + "'", e);
    }
  }
}
