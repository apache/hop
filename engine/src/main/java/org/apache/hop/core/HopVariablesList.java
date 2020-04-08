/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class HopVariablesList {

  //helper to make the HopVariablesList thread safe lazy initialized singleton
  private static class HopVariablesListHelper {
    private static final HopVariablesList INSTANCE = new HopVariablesList();
  }

  private static ILogChannel logger;

  private HopVariablesList() {
    logger = new LogChannel( this );
    descriptionMap = new HashMap<>();
    defaultValueMap = new HashMap<>();
  }

  public static HopVariablesList getInstance() {
    return HopVariablesListHelper.INSTANCE;
  }

  private Map<String, String> descriptionMap;
  private Map<String, String> defaultValueMap;

  public static void init() throws HopException {

    InputStream inputStream = null;
    try {
      HopVariablesList variablesList = getInstance();

      inputStream = variablesList.getClass().getResourceAsStream( Const.HOP_VARIABLES_FILE );

      if ( inputStream == null ) {
        inputStream = variablesList.getClass().getResourceAsStream( "/" + Const.HOP_VARIABLES_FILE );
      }
      if ( inputStream == null ) {
        throw new HopPluginException( "Unable to find standard kettle variables definition file: " + Const.HOP_VARIABLES_FILE );
      }
      Document doc = XmlHandler.loadXmlFile( inputStream, null, false, false );
      Node varsNode = XmlHandler.getSubNode( doc, "hop-variables" );
      int nrVars = XmlHandler.countNodes( varsNode, "hop-variable" );
      for ( int i = 0; i < nrVars; i++ ) {
        Node varNode = XmlHandler.getSubNodeByNr( varsNode, "hop-variable", i );
        String description = XmlHandler.getTagValue( varNode, "description" );
        String variable = XmlHandler.getTagValue( varNode, "variable" );
        String defaultValue = XmlHandler.getTagValue( varNode, "default-value" );

        variablesList.getDescriptionMap().put( variable, description );
        variablesList.getDefaultValueMap().put( variable, defaultValue );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to read file '" + Const.HOP_VARIABLES_FILE + "'", e );
    } finally {
      if ( inputStream != null ) {
        try {
          inputStream.close();
        } catch ( IOException e ) {
          // we do not able to close property file will log it
          logger.logDetailed( "Unable to close file kettle variables definition file", e );
        }
      }
    }
  }

  /**
   * @return A mapping between the name of a standard kettle variable and its description.
   */
  public Map<String, String> getDescriptionMap() {
    return descriptionMap;
  }

  /**
   * @return A mapping between the name of a standard kettle variable and its default value.
   */
  public Map<String, String> getDefaultValueMap() {
    return defaultValueMap;
  }

}
