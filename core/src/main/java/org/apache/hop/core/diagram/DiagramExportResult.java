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

package org.apache.hop.core.diagram;

import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;

/** Result of a diagram export operation. */
@Getter
@Setter
public class DiagramExportResult {
  private boolean success;
  private String filename;
  private String content;
  private byte[] bytes;
  private String mimeType;
  private String errorMessage;
  private Exception exception;

  public static DiagramExportResult success(String filename, String content, String mimeType) {
    DiagramExportResult result = new DiagramExportResult();
    result.setSuccess(true);
    result.setFilename(filename);
    result.setContent(content);
    if (content != null) {
      result.setBytes(content.getBytes(StandardCharsets.UTF_8));
    }
    result.setMimeType(mimeType);
    return result;
  }

  public static DiagramExportResult success(String filename, byte[] bytes, String mimeType) {
    DiagramExportResult result = new DiagramExportResult();
    result.setSuccess(true);
    result.setFilename(filename);
    result.setBytes(bytes);
    if (bytes != null) {
      result.setContent(new String(bytes, StandardCharsets.UTF_8));
    }
    result.setMimeType(mimeType);
    return result;
  }

  public static DiagramExportResult error(String errorMessage, Exception exception) {
    DiagramExportResult result = new DiagramExportResult();
    result.setSuccess(false);
    result.setErrorMessage(errorMessage);
    result.setException(exception);
    return result;
  }
}
