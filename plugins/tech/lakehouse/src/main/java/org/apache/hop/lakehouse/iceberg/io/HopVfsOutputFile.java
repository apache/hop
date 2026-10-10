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

import java.io.IOException;
import org.apache.commons.vfs2.FileObject;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/** An Iceberg {@link OutputFile} backed by a Hop VFS file. */
public class HopVfsOutputFile implements OutputFile {

  private final String location;

  public HopVfsOutputFile(String location) {
    this.location = location;
  }

  @Override
  public PositionOutputStream create() {
    FileObject file = HopVfsFileIO.resolve(location);
    try {
      if (file.exists()) {
        throw new AlreadyExistsException("File already exists: %s", location);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Unable to check %s", location);
    }
    return open(file);
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return open(HopVfsFileIO.resolve(location));
  }

  private PositionOutputStream open(FileObject file) {
    try {
      FileObject parent = file.getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
      return new CountingPositionOutputStream(file.getContent().getOutputStream(false));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Unable to create %s", location);
    }
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    return new HopVfsInputFile(location);
  }
}
