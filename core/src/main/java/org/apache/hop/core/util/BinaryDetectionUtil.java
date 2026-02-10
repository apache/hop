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

package org.apache.hop.core.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Simple heuristic to detect if file content looks like text (Option A: presence of null byte
 * indicates binary).
 */
public final class BinaryDetectionUtil {

  private static final int DEFAULT_MAX_BYTES = 8192;

  private BinaryDetectionUtil() {}

  /**
   * Returns true if the content appears to be text (no null byte in the sampled bytes). Returns
   * false if a null byte is found or on I/O error (treat as binary to be safe).
   *
   * @param inputStream stream to sample (will be read from; caller is responsible for closing)
   * @param maxBytesToSample maximum number of bytes to read (e.g. 8192)
   * @return true if no null byte was found in the first maxBytesToSample bytes, false otherwise
   */
  public static boolean looksLikeText(InputStream inputStream, int maxBytesToSample) {
    if (inputStream == null || maxBytesToSample <= 0) {
      return false;
    }
    byte[] buf = new byte[Math.min(maxBytesToSample, 65536)];
    try {
      int n = inputStream.read(buf, 0, buf.length);
      if (n <= 0) {
        return true;
      }
      for (int i = 0; i < n; i++) {
        if (buf[i] == 0) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /** Same as {@link #looksLikeText(InputStream, int)} with a default sample size of 8192 bytes. */
  public static boolean looksLikeText(InputStream inputStream) {
    return looksLikeText(inputStream, DEFAULT_MAX_BYTES);
  }
}
