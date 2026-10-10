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

package org.apache.hop.core.vfs;

import java.io.IOException;
import java.io.InputStream;

/**
 * A file on storage that reads a byte range with one request, such as an object store. A VFS
 * provider's file object implements it to say how large a piece it reads best, and to read exactly
 * that piece: {@link ReadAheadInputStream} cuts a file into pieces of that size and fetches several
 * at a time.
 */
public interface IRangeReadableFile {

  /**
   * The bytes per request this storage reads best, usually a setting of its connection.
   *
   * @return the read block size, at least 1
   */
  int getReadBlockSize();

  /**
   * Reads a byte range with one request, without fetching anything past it.
   *
   * @param offset the first byte to read
   * @param length the number of bytes to read; fewer come back at the end of the file
   * @return the bytes of the range
   * @throws IOException if the range can't be read
   */
  InputStream getInputStream(long offset, long length) throws IOException;
}
