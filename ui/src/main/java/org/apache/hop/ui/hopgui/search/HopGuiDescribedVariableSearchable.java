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

package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.ui.hopgui.HopGui;

import java.io.File;

public class HopGuiDescribedVariableSearchable implements ISearchable<DescribedVariable> {
  private DescribedVariable describedVariable;
  private String configFilename;

  public HopGuiDescribedVariableSearchable( DescribedVariable describedVariable, String configFilename ) {
    this.describedVariable = describedVariable;
    this.configFilename = configFilename;
  }

  @Override public String getLocation() {
    return "A variable in : " + (configFilename==null ? HopConfig.getInstance().getConfigFilename() : configFilename);
  }

  @Override public String getName() {
    return describedVariable.getName();
  }

  @Override public String getType() {
    return "Variable";
  }

  @Override public String getFilename() {
    return configFilename==null ? HopConfig.getInstance().getConfigFilename() : configFilename;
  }

  @Override public DescribedVariable getSearchableObject() {
    return describedVariable;
  }

  @Override public ISearchableCallback getSearchCallback() {
    return ( searchable, searchResult ) -> {

      String realConfigFilename = HopGui.getInstance().getVariables().resolve( configFilename );

      if (realConfigFilename==null) {
        HopGui.getInstance().menuToolsEditConfigVariables();
      } else {
        if (new File(realConfigFilename).exists()) {
          DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile( realConfigFilename );
          configFile.readFromFile();
          HopGui.editConfigFile( HopGui.getInstance().getShell(), realConfigFilename, configFile, searchResult.getComponent() );

          // TODO: if you change the file you want to refresh the project & environment
        }
      }
    };
  }
}
