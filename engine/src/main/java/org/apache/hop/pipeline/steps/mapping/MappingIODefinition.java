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
import org.apache.hop.pipeline.step.StepMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Helps to define the input or output specifications for the Mapping step.
 *
 * @author matt
 * @version 3.0
 * @since 2007-07-26
 */
public class MappingIODefinition implements Cloneable {

  public static final String XML_TAG = "mapping";

  private StepMeta inputStep;

  private String inputStepname;

  private String outputStepname;

  private String description;

  private List<MappingValueRename> valueRenames;

  private boolean mainDataPath;

  private boolean renamingOnOutput;

  /**
   * No input or output step is defined:<br>
   * - detect the source step automatically: use all input steps for this mapping step.<br>
   * - detect the output step automatically: there can only be one MappingInput step in the mapping in this specific
   * case.
   */
  public MappingIODefinition() {
    super();
    this.inputStepname = null;
    this.outputStepname = null;
    this.valueRenames = new ArrayList<MappingValueRename>();
    this.mainDataPath = false;
    this.renamingOnOutput = false;
  }

  /**
   * @param inputStepname  the name of the step to "connect" to. If no name is given, detect the source step automatically: use all
   *                       input steps for this mapping step.
   * @param outputStepname the name of the step in the mapping to accept the data from the input step. If no name is given, detect
   *                       the output step automatically: there can only be one MappingInput step in the mapping in this specific
   *                       case.
   */
  public MappingIODefinition( String inputStepname, String outputStepname ) {
    this();
    this.inputStepname = inputStepname;
    this.outputStepname = outputStepname;
  }

  @Override
  public Object clone() {
    try {
      MappingIODefinition definition = (MappingIODefinition) super.clone();
      return definition;
    } catch ( CloneNotSupportedException e ) {
      throw new RuntimeException( e ); // We don't want that in our code do we?
    }
  }

  public MappingIODefinition( Node mappingNode ) {

    this();

    inputStepname = XMLHandler.getTagValue( mappingNode, "input_step" );
    outputStepname = XMLHandler.getTagValue( mappingNode, "output_step" );
    mainDataPath = "Y".equalsIgnoreCase( XMLHandler.getTagValue( mappingNode, "main_path" ) );
    renamingOnOutput = "Y".equalsIgnoreCase( XMLHandler.getTagValue( mappingNode, "rename_on_output" ) );
    description = XMLHandler.getTagValue( mappingNode, "description" );

    int nrConnectors = XMLHandler.countNodes( mappingNode, "connector" );

    for ( int i = 0; i < nrConnectors; i++ ) {
      Node inputConnector = XMLHandler.getSubNodeByNr( mappingNode, "connector", i );
      String parentField = XMLHandler.getTagValue( inputConnector, "parent" );
      String childField = XMLHandler.getTagValue( inputConnector, "child" );
      valueRenames.add( new MappingValueRename( parentField, childField ) );
    }
  }

  public String getXML() {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "    " ).append( XMLHandler.openTag( XML_TAG ) );

    xml.append( "    " ).append( XMLHandler.addTagValue( "input_step", inputStepname ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "output_step", outputStepname ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "main_path", mainDataPath ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "rename_on_output", renamingOnOutput ) );
    xml.append( "    " ).append( XMLHandler.addTagValue( "description", description ) );

    for ( MappingValueRename valueRename : valueRenames ) {
      xml.append( "       " ).append( XMLHandler.openTag( "connector" ) );
      xml.append( XMLHandler.addTagValue( "parent", valueRename.getSourceValueName(), false ) );
      xml.append( XMLHandler.addTagValue( "child", valueRename.getTargetValueName(), false ) );
      xml.append( XMLHandler.closeTag( "connector" ) ).append( Const.CR );
    }

    xml.append( "    " ).append( XMLHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  /**
   * @return the stepname, the name of the step to "connect" to. If no step name is given, detect the Mapping
   * Input/Output step automatically.
   */
  public String getInputStepname() {
    return inputStepname;
  }

  /**
   * @param inputStepname the stepname to set
   */
  public void setInputStepname( String inputStepname ) {
    this.inputStepname = inputStepname;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description the description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * @return the outputStepname
   */
  public String getOutputStepname() {
    return outputStepname;
  }

  /**
   * @param outputStepname the outputStepname to set
   */
  public void setOutputStepname( String outputStepname ) {
    this.outputStepname = outputStepname;
  }

  /**
   * @return true if this is the main data path for the mapping step.
   */
  public boolean isMainDataPath() {
    return mainDataPath;
  }

  /**
   * @param mainDataPath true if this is the main data path for the mapping step.
   */
  public void setMainDataPath( boolean mainDataPath ) {
    this.mainDataPath = mainDataPath;
  }

  /**
   * @return the renamingOnOutput
   */
  public boolean isRenamingOnOutput() {
    return renamingOnOutput;
  }

  /**
   * @param renamingOnOutput the renamingOnOutput to set
   */
  public void setRenamingOnOutput( boolean renamingOnOutput ) {
    this.renamingOnOutput = renamingOnOutput;
  }

  /**
   * @return the valueRenames
   */
  public List<MappingValueRename> getValueRenames() {
    return valueRenames;
  }

  /**
   * @param valueRenames the valueRenames to set
   */
  public void setValueRenames( List<MappingValueRename> valueRenames ) {
    this.valueRenames = valueRenames;
  }

  /**
   * @return the inputStep
   */
  public StepMeta getInputStep() {
    return inputStep;
  }

  /**
   * @param inputStep the inputStep to set
   */
  public void setInputStep( StepMeta inputStep ) {
    this.inputStep = inputStep;
  }
}
