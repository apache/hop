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

package org.apache.hop.core;

import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HopVariablesList {

  private static HopVariablesList instance;

  private List<DescribedVariable> defaultVariables;

  private HopVariablesList() {
    defaultVariables = new ArrayList<>();
  }

  public static HopVariablesList getInstance() {
    return instance;
  }

  public static void init() throws HopException {

    instance = new HopVariablesList();

    InputStream inputStream = null;
    try {
      HopVariablesList variablesList = getInstance();

      inputStream = variablesList.getClass().getResourceAsStream( Const.HOP_VARIABLES_FILE );

      if ( inputStream == null ) {
        inputStream = variablesList.getClass().getResourceAsStream( "/" + Const.HOP_VARIABLES_FILE );
      }
      if ( inputStream == null ) {
        throw new HopPluginException( "Unable to find standard hop variables definition file: " + Const.HOP_VARIABLES_FILE );
      }
      Document doc = XmlHandler.loadXmlFile( inputStream, null, false, false );
      Node varsNode = XmlHandler.getSubNode( doc, "hop-variables" );
      int nrVars = XmlHandler.countNodes( varsNode, "hop-variable" );
      for ( int i = 0; i < nrVars; i++ ) {
        Node varNode = XmlHandler.getSubNodeByNr( varsNode, "hop-variable", i );
        String description = XmlHandler.getTagValue( varNode, "description" );
        String variable = XmlHandler.getTagValue( varNode, "variable" );
        String defaultValue = XmlHandler.getTagValue( varNode, "default-value" );

        instance.defaultVariables.add(new DescribedVariable(variable, defaultValue, description));
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to read file '" + Const.HOP_VARIABLES_FILE + "'", e );
    } finally {
      if ( inputStream != null ) {
        try {
          inputStream.close();
        } catch ( IOException e ) {
          // we do not able to close property file will log it
          LogChannel.GENERAL.logDetailed( "Unable to close file hop variables definition file", e );
        }
      }
    }
  }

  public DescribedVariable findEnvironmentVariable( String name) {
    for ( DescribedVariable describedVariable : defaultVariables) {
      if ( describedVariable.getName().equals( name )) {
        return describedVariable;
      }
    }
    return null;
  }

  public Set<String> getVariablesSet() {
    Set<String> variablesSet = new HashSet<>();
    for (DescribedVariable describedVariable : defaultVariables) {
      variablesSet.add(describedVariable.getName());
    }
    return variablesSet;
  }

  /**
   * Gets defaultVariables
   *
   * @return value of defaultVariables
   */
  public List<DescribedVariable> getEnvironmentVariables() {
    return defaultVariables;
  }

  /**
   * @param defaultVariables The defaultVariables to set
   */
  public void setDefaultVariables( List<DescribedVariable> defaultVariables ) {
    this.defaultVariables = defaultVariables;
  }

}
