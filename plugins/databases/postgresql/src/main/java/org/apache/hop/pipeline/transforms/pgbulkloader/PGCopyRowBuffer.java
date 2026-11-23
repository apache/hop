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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Collects the bytes of one COPY row so that it reaches the copy stream in a single write. Text is
 * encoded in the given charset (UTF-8 for the COPY); when it stores ASCII as ASCII, characters
 * below 0x80 are copied directly instead of through {@link String#getBytes(Charset)}.
 */
final class PGCopyRowBuffer {
  private final Charset charset;
  private final boolean asciiCompatible;
  private byte[] buffer = new byte[512];
  private int length;

  PGCopyRowBuffer(Charset charset) {
    this.charset = charset;
    this.asciiCompatible = isAsciiCompatible(charset);
  }

  /** True when every character below 0x80 encodes as that single byte, as in UTF-8 or Latin-1. */
  static boolean isAsciiCompatible(Charset charset) {
    if (!charset.canEncode()) {
      return false;
    }
    char[] ascii = new char[128];
    for (int c = 0; c < ascii.length; c++) {
      ascii[c] = (char) c;
    }
    byte[] bytes = new String(ascii).getBytes(charset);
    if (bytes.length != ascii.length) {
      return false;
    }
    for (int c = 0; c < bytes.length; c++) {
      if (bytes[c] != c) {
        return false;
      }
    }
    return true;
  }

  void reset() {
    length = 0;
  }

  int length() {
    return length;
  }

  private void ensure(int extra) {
    if (length + extra > buffer.length) {
      buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, length + extra));
    }
  }

  void append(byte[] bytes) {
    ensure(bytes.length);
    System.arraycopy(bytes, 0, buffer, length, bytes.length);
    length += bytes.length;
  }

  /** Appends the text as {@code text.getBytes(charset)} would encode it. */
  void append(CharSequence text) {
    append(text, 0, text.length());
  }

  /** Appends a range of the text as {@code text.subSequence(start, end).getBytes(charset)}. */
  void append(CharSequence text, int start, int end) {
    if (asciiCompatible) {
      int n = end - start;
      ensure(n);
      int pos = length;
      for (int i = start; i < end; i++) {
        char c = text.charAt(i);
        if (c >= 0x80) {
          // Leave anything else (and unpaired surrogates) to the charset, for the whole range.
          append(text.subSequence(start, end).toString().getBytes(charset));
          return;
        }
        buffer[pos++] = (byte) c;
      }
      length = pos;
    } else {
      append(text.subSequence(start, end).toString().getBytes(charset));
    }
  }

  void writeTo(OutputStream out) throws IOException {
    out.write(buffer, 0, length);
  }
}
