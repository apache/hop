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

package org.apache.hop.lakehouse.iceberg.io;

import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Iceberg {@link FileIO} on top of Hop VFS. Data and metadata files are read and written through
 * the VFS providers configured in Hop, so a table can live on any location Hop can reach (local
 * disk, S3, MinIO, Azure, Google Cloud Storage, ...) using the connections the user already has.
 *
 * <p>Use it by setting the catalog property {@code io-impl} to this class name.
 */
public class HopVfsFileIO implements FileIO {

  private Map<String, String> properties = Map.of();

  public HopVfsFileIO() {}

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = Map.copyOf(properties);
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new HopVfsInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new HopVfsInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new HopVfsOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    try {
      FileObject file = HopVfs.getFileObject(path);
      if (file.exists()) {
        file.delete();
      }
    } catch (Exception e) {
      throw new RuntimeIOException(new java.io.IOException("Unable to delete " + path, e));
    }
  }

  static FileObject resolve(String path) {
    try {
      return HopVfs.getFileObject(path);
    } catch (HopFileException e) {
      throw new RuntimeIOException(new java.io.IOException("Unable to resolve " + path, e));
    }
  }
}
