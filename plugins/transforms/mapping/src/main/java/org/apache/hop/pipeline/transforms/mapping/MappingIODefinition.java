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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Helps to define the input or output specifications for the Mapping transform.
 *
 * @author matt
 * @version 3.0
 * @since 2007-07-26
 *
 */
public class MappingIODefinition implements Cloneable {

  public static final String XML_TAG = "mapping";

  private TransformMeta inputTransform;

  private String inputTransformName;

  private String outputTransformName;

  private String description;

  private List<MappingValueRename> valueRenames;

  private boolean mainDataPath;

  private boolean renamingOnOutput;

  /**
   * No input or output transform is defined:<br>
   * - detect the source transform automatically: use all input transforms for this mapping transform.<br>
   * - detect the output transform automatically: there can only be one MappingInput transform in the mapping in this specific
   * case.
   */
  public MappingIODefinition() {
    super();
    this.inputTransformName = null;
    this.outputTransformName = null;
    this.valueRenames = new ArrayList<>();
    this.mainDataPath = false;
    this.renamingOnOutput = false;
  }

  /**
   * @param inputTransformName
   *          the name of the transform to "connect" to. If no name is given, detect the source transform automatically: use all
   *          input transforms for this mapping transform.
   * @param outputTransformName
   *          the name of the transform in the mapping to accept the data from the input transform. If no name is given, detect
   *          the output transform automatically: there can only be one MappingInput transform in the mapping in this specific
   *          case.
   */
  public MappingIODefinition( String inputTransformName, String outputTransformName ) {
    this();
    this.inputTransformName = inputTransformName;
    this.outputTransformName = outputTransformName;
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

    inputTransformName = XmlHandler.getTagValue( mappingNode, "input_transform" );
    outputTransformName = XmlHandler.getTagValue( mappingNode, "output_transform" );
    mainDataPath = "Y".equalsIgnoreCase( XmlHandler.getTagValue( mappingNode, "main_path" ) );
    renamingOnOutput = "Y".equalsIgnoreCase( XmlHandler.getTagValue( mappingNode, "rename_on_output" ) );
    description = XmlHandler.getTagValue( mappingNode, "description" );

    int nrConnectors = XmlHandler.countNodes( mappingNode, "connector" );

    for ( int i = 0; i < nrConnectors; i++ ) {
      Node inputConnector = XmlHandler.getSubNodeByNr( mappingNode, "connector", i );
      String parentField = XmlHandler.getTagValue( inputConnector, "parent" );
      String childField = XmlHandler.getTagValue( inputConnector, "child" );
      valueRenames.add( new MappingValueRename( parentField, childField ) );
    }
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder( 200 );

    xml.append( "    " ).append( XmlHandler.openTag( XML_TAG ) );

    xml.append( "    " ).append( XmlHandler.addTagValue( "input_transform", inputTransformName ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "output_transform", outputTransformName ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "main_path", mainDataPath ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "rename_on_output", renamingOnOutput ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "description", description ) );

    for ( MappingValueRename valueRename : valueRenames ) {
      xml.append( "       " ).append( XmlHandler.openTag( "connector" ) );
      xml.append( XmlHandler.addTagValue( "parent", valueRename.getSourceValueName(), false ) );
      xml.append( XmlHandler.addTagValue( "child", valueRename.getTargetValueName(), false ) );
      xml.append( XmlHandler.closeTag( "connector" ) ).append( Const.CR );
    }

    xml.append( "    " ).append( XmlHandler.closeTag( XML_TAG ) );

    return xml.toString();
  }

  /**
   * @return the TransformName, the name of the transform to "connect" to. If no transform name is given, detect the Mapping
   *         Input/Output transform automatically.
   */
  public String getInputTransformName() {
    return inputTransformName;
  }

  /**
   * @param inputTransformName
   *          the TransformName to set
   */
  public void setInputTransformName( String inputTransformName ) {
    this.inputTransformName = inputTransformName;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description
   *          the description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * @return the outputTransformName
   */
  public String getOutputTransformName() {
    return outputTransformName;
  }

  /**
   * @param outputTransformName
   *          the outputTransformName to set
   */
  public void setOutputTransformName( String outputTransformName ) {
    this.outputTransformName = outputTransformName;
  }

  /**
   * @return true if this is the main data path for the mapping transform.
   */
  public boolean isMainDataPath() {
    return mainDataPath;
  }

  /**
   * @param mainDataPath
   *          true if this is the main data path for the mapping transform.
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
   * @param renamingOnOutput
   *          the renamingOnOutput to set
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
   * @param valueRenames
   *          the valueRenames to set
   */
  public void setValueRenames( List<MappingValueRename> valueRenames ) {
    this.valueRenames = valueRenames;
  }

  /**
   * Gets inputTransform
   *
   * @return value of inputTransform
   */
  public TransformMeta getInputTransform() {
    return inputTransform;
  }

  /**
   * @param inputTransform The inputTransform to set
   */
  public void setInputTransform( TransformMeta inputTransform ) {
    this.inputTransform = inputTransform;
  }
}
