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

package org.apache.hop.pipeline.transforms.textfileoutput;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import org.apache.hop.core.util.Utils;

/**
 * Encodes text-file fragments without repeating a byte order mark.
 *
 * <p>{@code UTF-16}, {@code x-UTF-16LE-BOM}, {@code X-UTF-32BE-BOM} and {@code X-UTF-32LE-BOM}
 * prefix every {@code String.getBytes} result. Fragments are encoded with the matching charset that
 * does not write a mark. {@link TextFileOutput} writes that mark once, at the start of a new file.
 *
 * <p>{@code ISO-2022-KR}, {@code x-ISO-2022-CN-CNS} and {@code x-ISO-2022-CN-GB} can leave a
 * fragment in shift-out. A following separator encoded on its own is then read as a double-byte
 * character, so a fragment that ends shifted out is closed with shift-in.
 */
final class TextFileCharset {

  private static final byte SHIFT_OUT = 0x0E;
  private static final byte SHIFT_IN = 0x0F;

  private TextFileCharset() {}

  static byte[] byteOrderMark(String encoding) {
    String canonical = canonical(encoding);
    if (canonical == null) {
      return new byte[0];
    }
    return switch (canonical) {
      case "UTF-16" -> new byte[] {(byte) 0xFE, (byte) 0xFF};
      case "x-UTF-16LE-BOM" -> new byte[] {(byte) 0xFF, (byte) 0xFE};
      case "X-UTF-32BE-BOM" -> new byte[] {0x00, 0x00, (byte) 0xFE, (byte) 0xFF};
      case "X-UTF-32LE-BOM" -> new byte[] {(byte) 0xFF, (byte) 0xFE, 0x00, 0x00};
      default -> new byte[0];
    };
  }

  static byte[] encode(String text, String encoding) throws UnsupportedEncodingException {
    if (text == null) {
      return new byte[0];
    }
    byte[] encoded = text.getBytes(payloadCharset(encoding));
    if (endsShiftedOut(canonical(encoding), encoded)) {
      encoded = Arrays.copyOf(encoded, encoded.length + 1);
      encoded[encoded.length - 1] = SHIFT_IN;
    }
    return encoded;
  }

  /**
   * Drops one leading mark from a value that was encoded with {@code encoding} itself, which is
   * what {@code ValueMetaBase} does for non-string fields.
   */
  static byte[] stripLeadingByteOrderMark(byte[] encoded, String encoding) {
    if (encoded == null) {
      return null;
    }
    byte[] mark = byteOrderMark(encoding);
    if (mark.length == 0 || encoded.length < mark.length) {
      return encoded;
    }
    for (int i = 0; i < mark.length; i++) {
      if (encoded[i] != mark[i]) {
        return encoded;
      }
    }
    return Arrays.copyOfRange(encoded, mark.length, encoded.length);
  }

  private static Charset payloadCharset(String encoding) throws UnsupportedEncodingException {
    String canonical = canonical(encoding);
    if (canonical == null) {
      throw unsupported(encoding, null);
    }
    String payload =
        switch (canonical) {
          case "UTF-16" -> "UTF-16BE";
          case "x-UTF-16LE-BOM" -> "UTF-16LE";
          case "X-UTF-32BE-BOM" -> "UTF-32BE";
          case "X-UTF-32LE-BOM" -> "UTF-32LE";
          default -> canonical;
        };
    try {
      return Charset.forName(payload);
    } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
      throw unsupported(encoding, e);
    }
  }

  private static boolean endsShiftedOut(String canonical, byte[] encoded) {
    if (!("ISO-2022-KR".equals(canonical)
        || "x-ISO-2022-CN-CNS".equals(canonical)
        || "x-ISO-2022-CN-GB".equals(canonical))) {
      return false;
    }
    boolean shiftedOut = false;
    for (byte unit : encoded) {
      if (unit == SHIFT_OUT) {
        shiftedOut = true;
      } else if (unit == SHIFT_IN) {
        shiftedOut = false;
      }
    }
    return shiftedOut;
  }

  private static String canonical(String encoding) {
    if (Utils.isEmpty(encoding)) {
      return null;
    }
    try {
      return Charset.forName(encoding).name();
    } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
      return null;
    }
  }

  private static UnsupportedEncodingException unsupported(String encoding, Exception cause) {
    UnsupportedEncodingException error = new UnsupportedEncodingException(encoding);
    if (cause != null) {
      error.initCause(cause);
    }
    return error;
  }
}
