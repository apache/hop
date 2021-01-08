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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

/**
 * The workflow to be executed in the pipeline can receive parameters. These are either coming from an input row (the
 * first row in a group of rows) or from a static variable or value.
 *
 * @author matt
 * @version 4.3
 * @since 2011-AUG-29
 */
public class WorkflowExecutorParameters implements Cloneable {

  public static final String XML_TAG = "parameters";

  private static final String XML_VARIABLES_TAG = "variablemapping";

  /**
   * The name of the variable to set in the workflow
   */
  private String[] variable;

  private String[] field;

  /**
   * This is a simple String with optionally variables in them
   **/
  private String[] input;

  /**
   * This flag causes the workflow to inherit all variables from the parent pipeline
   */
  private boolean inheritingAllVariables;

  public WorkflowExecutorParameters() {
    super();

    variable = new String[] {};
    field = new String[] {};
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

  public WorkflowExecutorParameters( Node paramNode ) {

    int nrVariables = XmlHandler.countNodes( paramNode, XML_VARIABLES_TAG );
    variable = new String[ nrVariables ];
    field = new String[ nrVariables ];
    input = new String[ nrVariables ];

    for ( int i = 0; i < variable.length; i++ ) {
      Node variableMappingNode = XmlHandler.getSubNodeByNr( paramNode, XML_VARIABLES_TAG, i );

      variable[ i ] = XmlHandler.getTagValue( variableMappingNode, "variable" );
      field[ i ] = XmlHandler.getTagValue( variableMappingNode, "field" );
      input[ i ] = XmlHandler.getTagValue( variableMappingNode, "input" );
    }

    inheritingAllVariables = "Y".equalsIgnoreCase( XmlHandler.getTagValue( paramNode, "inherit_all_vars" ) );
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "    " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );

    for ( int i = 0; i < variable.length; i++ ) {
      xml.append( "      " ).append( XmlHandler.openTag( XML_VARIABLES_TAG ) );
      xml.append( XmlHandler.addTagValue( "variable", variable[ i ], false ) );
      xml.append( XmlHandler.addTagValue( "field", field[ i ], false ) );
      xml.append( XmlHandler.addTagValue( "input", input[ i ], false ) );
      xml.append( XmlHandler.closeTag( XML_VARIABLES_TAG ) ).append( Const.CR );
    }
    xml.append( "      " ).append( XmlHandler.addTagValue( "inherit_all_vars", inheritingAllVariables ) );
    xml.append( "    " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return xml.toString();
  }

  /**
   * @return the field name to use
   */
  public String[] getField() {
    return field;
  }

  /**
   * @param field the input field name to set
   */
  public void setField( String[] field ) {
    this.field = field;
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

  /**
   * @return the input
   */
  public String[] getInput() {
    return input;
  }

  /**
   * @param input the input to set
   */
  public void setInput( String[] input ) {
    this.input = input;
  }
}
