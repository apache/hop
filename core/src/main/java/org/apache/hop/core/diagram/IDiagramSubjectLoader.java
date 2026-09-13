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
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Interface allowing plugins/modules to load a domain subject (such as a pipeline, workflow, or
 * model) from a file on disk/VFS for diagram export.
 */
public interface IDiagramSubjectLoader {
  /**
   * Checks if this loader supports the given filename/URI.
   *
   * @param filename the file path or URI
   * @return true if this loader can parse the file
   */
  boolean supportsFile(String filename);

  /**
   * Loads the domain subject from the file.
   *
   * @param filename the file path or URI
   * @param metadataProvider the metadata provider
   * @param variables variables for resolving paths and parameters
   * @return the loaded domain subject
   * @throws HopException in case loading fails
   */
  Object loadSubject(String filename, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException;
}
