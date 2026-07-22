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

package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsMetadataExporter;

/**
 * When a project is enabled, optionally write/update the project's single-file metadata export
 * (issue #3553).
 */
@ExtensionPoint(
    id = "AutoExportMetadataOnProjectActivated",
    description = "Auto-export project metadata to a single JSON file when a project is activated",
    extensionPointId = Defaults.EXTENSION_POINT_PROJECT_ACTIVATED)
public class AutoExportMetadataOnProjectActivated implements IExtensionPoint<Object> {
  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object projectName)
      throws HopException {
    ProjectsMetadataExporter.autoExportActiveProjectIfEnabled(log, variables);
  }
}
