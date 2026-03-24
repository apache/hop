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
 *
 */

package org.apache.hop.pipeline.transforms.fileinput;

import java.io.InputStreamReader;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputMeta;

/** An older utility class. A left-over of the deprecated and removed Text File Input transform. */
public class LineReader {
  private LineReader() {
    /* This utility class should not be instantiated */
  }

  private static final Class<?> PKG = LineReader.class;

  public static String getLine(
      ILogChannel log, InputStreamReader reader, int formatNr, StringBuilder line)
      throws HopFileException {
    EncodingType type = EncodingType.guessEncodingType(reader.getEncoding());
    return getLine(log, reader, type, formatNr, line);
  }

  public static String getLine(
      ILogChannel log,
      InputStreamReader reader,
      EncodingType encodingType,
      int formatNr,
      StringBuilder line)
      throws HopFileException {
    int c = 0;
    line.setLength(0);
    try {
      switch (formatNr) {
        case TextFileInputMeta.FILE_FORMAT_DOS:
          while (c >= 0) {
            c = reader.read();

            if (encodingType.isReturn(c) || encodingType.isLinefeed(c)) {
              c = reader.read(); // skip \n and \r
              if (!encodingType.isReturn(c) && !encodingType.isLinefeed(c)) {
                // make sure its really a linefeed or cariage return
                // raise an error this is not a DOS file
                // so we have pulled a character from the next line
                throw new HopFileException(
                    BaseMessages.getString(PKG, "TextFileInput.Log.SingleLineFound"));
              }
              return line.toString();
            }
            if (c >= 0) {
              line.append((char) c);
            }
          }
          break;
        case TextFileInputMeta.FILE_FORMAT_UNIX:
          while (c >= 0) {
            c = reader.read();

            if (encodingType.isLinefeed(c) || encodingType.isReturn(c)) {
              return line.toString();
            }
            if (c >= 0) {
              line.append((char) c);
            }
          }
          break;
        case TextFileInputMeta.FILE_FORMAT_MIXED:
          // in mixed mode we suppose the LF is the last char and CR is ignored
          // not for MAC OS 9 but works for Mac OS X. Mac OS 9 can use UNIX-Format
          while (c >= 0) {
            c = reader.read();

            if (encodingType.isLinefeed(c)) {
              return line.toString();
            } else if (!encodingType.isReturn(c) && c >= 0) {
              line.append((char) c);
            }
          }
          break;
        default:
          break;
      }
    } catch (HopFileException e) {
      throw e;
    } catch (Exception e) {
      if (line.isEmpty()) {
        throw new HopFileException(
            BaseMessages.getString(
                PKG, "TextFileInput.Log.Error.ExceptionReadingLine", e.toString()),
            e);
      }
      return line.toString();
    }
    if (!line.isEmpty()) {
      return line.toString();
    }

    return null;
  }
}
