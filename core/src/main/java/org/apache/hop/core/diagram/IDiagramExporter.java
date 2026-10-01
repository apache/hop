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

import org.apache.hop.core.exception.HopException;

/**
 * Common interface for diagram exporters.
 *
 * @param <T> the type of subject this exporter can handle
 */
public interface IDiagramExporter<T> {
  /**
   * @return the unique exporter identifier
   */
  String getId();

  /**
   * @return the display name of the exporter
   */
  String getName();

  /**
   * @return a description of this exporter
   */
  String getDescription();

  /**
   * @return the target format
   */
  DiagramExportFormat getFormat();

  /**
   * @return the default file extension without dot (e.g. "svg", "mmd")
   */
  String getFileExtension();

  /**
   * @return file filter names for file dialogs
   */
  String[] getFileFilterNames();

  /**
   * Checks if this exporter can handle the specified subject instance.
   *
   * @param subject the object to check
   * @return true if supported
   */
  boolean supportsSubject(Object subject);

  /**
   * Performs the diagram export.
   *
   * @param subject the subject being exported
   * @param options the export options
   * @param context the execution context
   * @return the export result
   * @throws HopException in case of error
   */
  DiagramExportResult export(T subject, DiagramExportOptions options, IExportContext context)
      throws HopException;

  /**
   * Options bean class for dialog presentation.
   *
   * @return the options class
   */
  default Class<? extends DiagramExportOptions> getOptionsClass() {
    return DiagramExportOptions.class;
  }
}
