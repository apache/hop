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

package org.apache.hop.config;

import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import picocli.CommandLine;

@ConfigPlugin(
  id="SetHopConfigVariables",
  description = "Set system properties"
)
public class SetHopConfigVariables implements IConfigOptions {

  @CommandLine.Option( names = { "-sv", "--set-variable" }, description = "Set a variable, use format VAR=Value" )
  private String setVariable = null;

  @CommandLine.Option( names = { "-dv", "--describe-variable" }, description = "Describe a variable, use format VARIABLE=Description" )
  private String describeVariable = null;

  @Override public boolean handleOption( ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    // Is this an option we want to handle?
    //
    boolean changed = false;
    if (setVariable!=null) {
      int equalsIndex = setVariable.indexOf( '=' );
      if ( equalsIndex > 0 ) {
        String name = setVariable.substring( 0, equalsIndex );
        String value = setVariable.substring( equalsIndex + 1 );

        DescribedVariable describedVariable = HopConfig.getInstance().findDescribedVariable( name );
        if ( describedVariable ==null) {
          describedVariable = new DescribedVariable(name, value, null);
        } else {
          describedVariable.setValue( value );
        }
        HopConfig.getInstance().setDescribedVariable( describedVariable );
        changed = true;
      } else {
        throw new HopException("Please set a variable value in the format 'VARIABLE_NAME=VALUE'");
      }
    }
    if (describeVariable!=null) {
      int equalsIndex = describeVariable.indexOf( '=' );
      if ( equalsIndex > 0 ) {
        String name = describeVariable.substring( 0, equalsIndex );
        String description = describeVariable.substring( equalsIndex + 1 );

        DescribedVariable describedVariable = HopConfig.getInstance().findDescribedVariable( name );
        if ( describedVariable ==null) {
          describedVariable = new DescribedVariable(name, null, description);
        } else {
          describedVariable.setDescription( description );
        }
        HopConfig.getInstance().setDescribedVariable( describedVariable );
      } else {
        throw new HopException("Please set a variable description in the format 'VARIABLE_NAME=DESCRIPTION'");
      }
    }
    return false;
  }
}

