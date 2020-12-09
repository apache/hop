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
import org.apache.hop.metadata.util.ReflectionUtil;
import org.apache.hop.projects.util.ProjectsUtil;

@ExtensionPoint(
  id = "GitRepositoryCreateExtensionPoint",
  extensionPointId = "GitRepositoryCreate",
  description = "Sets the name and base folder of a new git repository to the base of the project"
)
public class GitRepositoryCreateExtensionPoint implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, Object project ) throws HopException {
    ReflectionUtil.setFieldValue( project, "name", String.class, "git");
    ReflectionUtil.setFieldValue( project, "directory", String.class, "${"+ ProjectsUtil.VARIABLE_PROJECT_HOME+"}");
  }
}
