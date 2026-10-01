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

package org.apache.hop.diagram.loader;

import org.apache.hop.core.diagram.IDiagramSubjectLoader;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;

/** Loads a {@link WorkflowMeta} domain subject from a .hwf workflow file. */
public class WorkflowDiagramSubjectLoader implements IDiagramSubjectLoader {

  @Override
  public boolean supportsFile(String filename) {
    return filename != null && filename.toLowerCase().endsWith(".hwf");
  }

  @Override
  public Object loadSubject(
      String filename, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    if (metadataProvider == null) {
      metadataProvider = org.apache.hop.metadata.util.HopMetadataInstance.getMetadataProvider();
      if (metadataProvider == null) {
        metadataProvider = new org.apache.hop.metadata.serializer.json.JsonMetadataProvider();
      }
    }
    return new WorkflowMeta(variables, filename, metadataProvider);
  }
}
