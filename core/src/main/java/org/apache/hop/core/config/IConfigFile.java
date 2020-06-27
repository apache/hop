package org.apache.hop.core.config;

import java.util.List;

public interface IConfigFile {

  String getConfigFilename();
  void setConfigFilename( String filename );

  List<DescribedVariable> getDescribedVariables();
  DescribedVariable findDescribedVariable( String name);
  void setDescribedVariable( DescribedVariable variable );
  String findDescribedVariableValue( String name);
  void setDescribedVariables( List<DescribedVariable> describedVariables );
}
