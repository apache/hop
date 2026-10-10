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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.commons.vfs2.FileObject;

/**
 * A piece of a file one transform copy reads when Text File Input runs in parallel: the lines whose
 * first byte lies in {@code [start, end)}, or exactly those bytes for an exact chunk.
 *
 * @param file the file
 * @param start the first byte of the range
 * @param end the byte after the range
 * @param wholeFile the file is read as one piece, from its first line, as without parallel reading
 * @param charset the encoding of a piece that starts inside the file, where no byte order mark can
 *     be seen; null for a piece that starts the file
 * @param lineFeed the line feed in that encoding: one byte, or two for UTF-16
 * @param exact start and end are where records start, found by reading the file first: the chunk is
 *     exactly the bytes in {@code [start, end)}
 * @param passThruRow the row of the previous transform that named the file, whose fields are passed
 *     through to every row read from it; null when the files don't come from a previous transform
 */
public record TextFileChunk(
    FileObject file,
    long start,
    long end,
    boolean wholeFile,
    String charset,
    byte[] lineFeed,
    boolean exact,
    Object[] passThruRow) {

  /** The whole file, read from its first line by one copy. */
  static TextFileChunk whole(FileObject file, long size, Object[] passThruRow) {
    return new TextFileChunk(file, 0, size, true, null, null, false, passThruRow);
  }

  /** This piece starts at the beginning of the file, where the byte order mark and header are. */
  boolean startsFile() {
    return start == 0;
  }
}
