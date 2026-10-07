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
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/** An Iceberg {@link InputFile} backed by a Hop VFS file. */
public class HopVfsInputFile implements InputFile {

  private final String location;
  private Long length;

  public HopVfsInputFile(String location) {
    this.location = location;
  }

  public HopVfsInputFile(String location, long length) {
    this.location = location;
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == null) {
      try {
        FileObject file = HopVfsFileIO.resolve(location);
        if (!file.exists()) {
          throw new NotFoundException("File does not exist: %s", location);
        }
        length = file.getContent().getSize();
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Unable to get the size of %s", location);
      }
    }
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    FileObject file = HopVfsFileIO.resolve(location);
    try {
      if (!file.exists()) {
        throw new NotFoundException("File does not exist: %s", location);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Unable to open %s", location);
    }
    return new HopVfsSeekableInputStream(file);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    try {
      return HopVfsFileIO.resolve(location).exists();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Unable to check %s", location);
    }
  }
}
