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

package org.apache.hop.pipeline.steps.mapping;

import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XMLHandler;
import org.w3c.dom.Node;

/**
 * We need out mapping to be parameterized.<br>
 * This we do with the use of environment variables.<br>
 * That way we can set one variable to another, etc.<br>
 *
 * @author matt
 * @version 3.0
 * @since 2007-06-27
 */
public class MappingParameters implements Cloneable {

  public static final String XML_TAG = "parameters";

  private static final String XML_VARIABLES_TAG = "variablemapping";

  /**
   * The name of the variable to set in the sub-pipeline
   */
  private String[] variable;

  /**
   * This is a simple String with optionally variables in them
   **/
  private String[] input;

  /**
   * This flag causes the sub- pipeline to inherit all variables from the parent
   */
  private boolean inheritingAllVariables;

  public MappingParameters() {
    super();

    variable = new String[] {};
    input = new String[] {};

    inheritingAllVariables = true;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      throw new RuntimeException( e ); // Nope, we don't want that in our code.
    }
  }

  public MappingParameters( Node paramNode ) {

    int nrVariables = XMLHandler.countNodes( paramNode, XML_VARIABLES_TAG );
    variable = new String[ nrVariables ];
    input = new String[ nrVariables ];

    for ( int i = 0; i < variable.length; i++ ) {
      Node variableMappingNode = XMLHandler.getSubNodeByNr( paramNode, XML_VARIABLES_TAG, i );

      variable[ i ] = XMLHandler.getTagValue( variableMappingNode, "variable" );
      input[ i ] = XMLHandler.getTagValue( variableMappingNode, "input" );
    }

    inheritingAllVariables = "Y".equalsIgnoreCase( XMLHandler.getTagValue( paramNode, "inherit_all_vars" ) );
  }

  public String getXML() {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "    " ).append( XMLHandler.openTag( XML_TAG ) );

    for ( int i = 0; i < variable.length; i++ ) {
      xml.append( "       " ).append( XMLHandler.openTag( XML_VARIABLES_TAG ) );
      xml.append( XMLHandler.addTagValue( "variable", variable[ i ], false ) );
      xml.append( XMLHandler.addTagValue( "input", input[ i ], false ) );
      xml.append( XMLHandler.closeTag( XML_VARIABLES_TAG ) ).append( Const.CR );
    }
    xml.append( "    " ).append( XMLHandler.addTagValue( "inherit_all_vars", inheritingAllVariables ) );
    xml.append( "    " ).append( XMLHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  /**
   * @return the inputField
   */
  public String[] getInputField() {
    return input;
  }

  /**
   * @param inputField the inputField to set
   */
  public void setInputField( String[] inputField ) {
    this.input = inputField;
  }

  /**
   * @return the variable
   */
  public String[] getVariable() {
    return variable;
  }

  /**
   * @param variable the variable to set
   */
  public void setVariable( String[] variable ) {
    this.variable = variable;
  }

  /**
   * @return the inheritingAllVariables
   */
  public boolean isInheritingAllVariables() {
    return inheritingAllVariables;
  }

  /**
   * @param inheritingAllVariables the inheritingAllVariables to set
   */
  public void setInheritingAllVariables( boolean inheritingAllVariables ) {
    this.inheritingAllVariables = inheritingAllVariables;
  }

}
