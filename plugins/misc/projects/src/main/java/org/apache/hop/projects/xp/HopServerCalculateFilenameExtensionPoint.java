/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.xp;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.run.HopRun;
import org.apache.hop.www.HopServer;

@ExtensionPoint(
    id = "HopServerCalculateFilenameExtensionPoint",
    extensionPointId = "HopServerCalculateFilename",
    description = "Resolves configuration filename specified relative to the given project")
public class HopServerCalculateFilenameExtensionPoint implements IExtensionPoint<HopServer> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, HopServer hopServer)
      throws HopException {

    if (hopServer.getParameters() == null || hopServer.getParameters().size() == 1)
      try {
        String filename = variables.resolve(hopServer.getParameters().get(0));
        FileObject fileObject = HopVfs.getFileObject(filename);
        if (!fileObject.exists()) {
          // Try to prepend with ${PROJECT_HOME}
          //
          String alternativeFilename = variables.resolve("${PROJECT_HOME}/" + filename);
          fileObject = HopVfs.getFileObject(alternativeFilename);
          if (fileObject.exists()) {
            hopServer.setRealFilename(alternativeFilename);
            log.logBasic("Relative path filename specified: " + hopServer.getRealFilename());
          }
        }
      } catch (Exception e) {
        throw new HopException("Error calculating configuration filename (relative to project)", e);
      }
  }
}
