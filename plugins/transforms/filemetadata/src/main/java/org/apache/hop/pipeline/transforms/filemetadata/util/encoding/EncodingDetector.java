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

package org.apache.hop.pipeline.transforms.filemetadata.util.encoding;

import org.mozilla.universalchardet.UniversalDetector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class EncodingDetector {

  public static Charset detectEncoding(InputStream inputStream, Charset defaultCharset, long limitSize) throws IOException {

    UniversalDetector detector = new UniversalDetector(null);
    byte[] buf = new byte[4096];
    long totalBytesRead = 0;

    String charsetName;

    try {
      int bytesRead = 0;
      while ((limitSize <= 0 || totalBytesRead <= limitSize) && (bytesRead = inputStream.read(buf)) > 0 && !detector.isDone()) {
        detector.handleData(buf, 0, bytesRead);
        totalBytesRead += bytesRead;
      }

      detector.isDone();
      detector.handleData(buf, 0, bytesRead);

      detector.dataEnd();
      charsetName = detector.getDetectedCharset();
      detector.reset();
      // sadly, the lib can through almost anything
      // because of unexpected results in probing
    } catch (Throwable e) {
      charsetName = null;
    }

    if (charsetName != null && Charset.isSupported(charsetName)) {
      return Charset.forName(charsetName);
    } else {
      return defaultCharset;
    }

  }

}
