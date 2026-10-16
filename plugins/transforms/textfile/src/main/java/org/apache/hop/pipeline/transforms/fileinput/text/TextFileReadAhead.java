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

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;

/**
 * How Text File Input fetches a file on remote storage ahead of the rows it reads: {@code blocks}
 * blocks in flight, each by its own ranged request. The block size is the storage's own, from its
 * connection (see {@link HopVfs#getReadBlockSize(FileObject)}). Local files and storage that can't
 * start reading at an offset are read as a plain stream.
 *
 * @param blocks the blocks in flight per file or chunk being read; 0 reads without fetching ahead
 */
public record TextFileReadAhead(int blocks) {

  /**
   * Off: each copy reads on its own thread only. Fetching ahead uses download threads besides the
   * copy's, so it is switched on per transform.
   */
  public static final int DEFAULT_BLOCKS = 0;

  static final int MAX_BLOCKS = 64;

  /** Reading without fetching ahead. */
  public static final TextFileReadAhead NONE = new TextFileReadAhead(0);

  /**
   * Parses the setting. Blank reads without fetching ahead, like the default of 0.
   *
   * @param blocks the number of blocks in flight, from 0 to 64
   */
  public static TextFileReadAhead parse(String blocks) throws HopException {
    if (StringUtils.isBlank(blocks)) {
      return NONE;
    }
    int nrBlocks;
    try {
      nrBlocks = Integer.parseInt(blocks.trim());
    } catch (NumberFormatException e) {
      nrBlocks = -1;
    }
    if (nrBlocks < 0 || nrBlocks > MAX_BLOCKS) {
      throw new HopException(
          "Invalid number of read-ahead blocks '"
              + blocks
              + "': use a number from 0 (don't fetch ahead) to "
              + MAX_BLOCKS);
    }
    return new TextFileReadAhead(nrBlocks);
  }

  /**
   * Opens a file at an offset, fetching ahead up to {@code readAheadEnd}.
   *
   * @param file the file
   * @param offset the first byte to read
   * @param readAheadEnd how far to fetch ahead: the end of the part of the file to read
   */
  public InputStream open(FileObject file, long offset, long readAheadEnd) throws IOException {
    return HopVfs.getReadAheadInputStream(file, offset, readAheadEnd, blocks);
  }

  /** Opens a whole file, fetching ahead all the way; without read-ahead, as a plain stream. */
  public InputStream openWholeFile(FileObject file) throws IOException {
    return blocks < 1 ? HopVfs.getInputStream(file) : open(file, 0, Long.MAX_VALUE);
  }
}
