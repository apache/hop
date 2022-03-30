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

import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

@ExtensionPoint(
    id = "HopImportVariables",
    description = "Imports variables into a Hop project",
    extensionPointId = "HopImportVariables")
public class HopImportVariables implements IExtensionPoint<Object[]> {

  @Override
  public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, Object[] payload)
      throws HopException {

    String configFilename = (String) payload[0];
    IVariables collectedVariables = (IVariables) payload[1];

    try {

      DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(configFilename);
      if (HopVfs.fileExists(configFilename)) {
        configFile.readFromFile();
      }
      for (String variableName : collectedVariables.getVariableNames()) {
        String value = collectedVariables.getVariable(variableName);
        DescribedVariable describedVariable = configFile.findDescribedVariable(variableName);
        if (describedVariable == null) {
          describedVariable = new DescribedVariable();
          describedVariable.setDescription("Imported from Kettle");
        }
        describedVariable.setName(variableName);
        describedVariable.setValue(value);
        configFile.setDescribedVariable(describedVariable);
      }

      // Save the file...
      //
      configFile.saveToFile();
    } catch (Exception e) {
      throw new HopException(
          "Error importing variables to environment config file '" + configFilename, e);
    }
  }
}
