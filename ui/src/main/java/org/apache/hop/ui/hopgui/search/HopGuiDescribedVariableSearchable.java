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
    return "A variable in : " + HopConfig.getInstance().getConfigFilename();
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
      if (configFilename==null) {
        HopGui.getInstance().menuToolsEditConfigVariables();
      } else {
        if (new File(configFilename).exists()) {
          DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile( configFilename );
          configFile.readFromFile();
          HopGui.editConfigFile( HopGui.getInstance().getShell(), configFilename, configFile );
        }
      }
    };
  }
}
