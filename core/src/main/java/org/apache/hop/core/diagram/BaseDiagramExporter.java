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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Base class for diagram exporters providing default annotation reading, subject support checking,
 * and HopVfs file writing utilities.
 *
 * @param <T> the type of subject
 */
@Getter
@Setter
public abstract class BaseDiagramExporter<T> implements IDiagramExporter<T> {
  protected String id;
  protected String name;
  protected String description;
  protected DiagramExportFormat format;
  protected String fileExtension;
  protected String[] fileFilterNames;
  protected Class<?>[] supportedSubjectTypes;

  protected BaseDiagramExporter() {
    DiagramExporter annotation = getClass().getAnnotation(DiagramExporter.class);
    if (annotation != null) {
      this.id = annotation.id();
      this.name = annotation.name();
      this.description = annotation.description();
      this.format = DiagramExportFormat.parse(annotation.format());
      this.fileExtension = annotation.fileExtension();
      this.fileFilterNames = annotation.fileFilterNames();
      this.supportedSubjectTypes = annotation.supportedSubjectTypes();
    }
  }

  @Override
  public boolean supportsSubject(Object subject) {
    if (subject == null) {
      return false;
    }
    if (supportedSubjectTypes != null && supportedSubjectTypes.length > 0) {
      for (Class<?> type : supportedSubjectTypes) {
        if (type.isInstance(subject)) {
          return true;
        }
      }
    }
    return false;
  }

  protected void writeToTarget(String targetFilename, byte[] data, IExportContext context)
      throws HopException {
    if (StringUtils.isBlank(targetFilename) || data == null) {
      return;
    }
    try {
      String resolved =
          context != null && context.getVariables() != null
              ? context.getVariables().resolve(targetFilename)
              : targetFilename;
      FileObject fileObject = HopVfs.getFileObject(resolved);
      try (OutputStream out = HopVfs.getOutputStream(fileObject, false)) {
        out.write(data);
      }
    } catch (Exception e) {
      throw new HopException("Error writing export file to " + targetFilename, e);
    }
  }

  protected void writeToTarget(String targetFilename, String content, IExportContext context)
      throws HopException {
    if (content != null) {
      writeToTarget(targetFilename, content.getBytes(StandardCharsets.UTF_8), context);
    }
  }
}
