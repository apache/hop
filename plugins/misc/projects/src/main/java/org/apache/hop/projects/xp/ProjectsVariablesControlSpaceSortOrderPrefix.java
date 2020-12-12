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
import org.apache.hop.projects.util.ProjectsUtil;

import java.util.Map;

@ExtensionPoint(
  id = "ProjectsVariablesControlSpaceSortOrderPrefix",
  extensionPointId = "HopGuiGetControlSpaceSortOrderPrefix",
  description = "Set a prefix sort order for the projects variables, push to front of the list"
)
public class ProjectsVariablesControlSpaceSortOrderPrefix
    implements IExtensionPoint<Map<String, String>> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, Map<String, String> prefixMap) throws HopException {

    prefixMap.put(ProjectsUtil.VARIABLE_PROJECT_HOME, "310_");
    prefixMap.put( Defaults.VARIABLE_HOP_PROJECT_NAME, "450_");
    prefixMap.put( Defaults.VARIABLE_HOP_ENVIRONMENT_NAME, "450_");
    prefixMap.put( ProjectsUtil.VARIABLE_HOP_DATASETS_FOLDER, "450_");
    prefixMap.put( ProjectsUtil.VARIABLE_HOP_UNIT_TESTS_FOLDER, "460_");

  }
}
