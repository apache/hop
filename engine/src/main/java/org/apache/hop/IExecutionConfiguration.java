/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop;

import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;

import java.io.IOException;
import java.util.Map;

public interface IExecutionConfiguration extends Cloneable {

  Object clone();

  LogLevel getLogLevel();

  void setLogLevel( LogLevel logLevel );

  Map<String, String> getParametersMap();

  void setParametersMap( Map<String, String> parametersMap );

  Result getPreviousResult();

  void setPreviousResult( Result previousResult );

  Map<String, String> getVariablesMap();

  void setVariablesMap( Map<String, String> variablesMap );

  void setVariablesMap( IVariables variablesMap );

  String getXml() throws IOException;

  boolean isClearingLog();

  void setClearingLog( boolean clearingLog );

  String getRunConfiguration();

  void setRunConfiguration( String runConfiguration );
}
