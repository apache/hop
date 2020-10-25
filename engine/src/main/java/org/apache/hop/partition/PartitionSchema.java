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

package org.apache.hop.partition;

import org.apache.hop.core.Const;
import org.apache.hop.core.changed.ChangedFlag;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A partition schema allow you to partition a transform according into a number of partitions that run independendly. It
 * allows us to "map"
 *
 * @author Matt
 */
@HopMetadata(
  key = "partition",
  name = "Partition Schema",
  description = "Describes a partition schema",
  iconImage = "ui/images/partition_schema.svg"
)
public class PartitionSchema extends ChangedFlag implements Cloneable, IVariables, IHopMetadata {

  @HopMetadataProperty
  private String name;

  @HopMetadataProperty
  private List<String> partitionIDs;

  @HopMetadataProperty
  private boolean dynamicallyDefined;

  @HopMetadataProperty
  private String numberOfPartitions;

  private IVariables variables = new Variables();

  public PartitionSchema() {
    this.dynamicallyDefined = true;
    this.numberOfPartitions = "4";
    this.partitionIDs = new ArrayList<>();
  }

  /**
   * @param name
   * @param partitionIDs
   */
  public PartitionSchema( String name, List<String> partitionIDs ) {
    this.name = name;
    this.partitionIDs = partitionIDs;
  }

  public Object clone() {
    PartitionSchema partitionSchema = new PartitionSchema();
    partitionSchema.replaceMeta( this );
    return partitionSchema;
  }

  public void replaceMeta( PartitionSchema partitionSchema ) {
    this.name = partitionSchema.name;
    this.partitionIDs = new ArrayList<>();
    this.partitionIDs.addAll( partitionSchema.partitionIDs );

    this.dynamicallyDefined = partitionSchema.dynamicallyDefined;
    this.numberOfPartitions = partitionSchema.numberOfPartitions;

    this.setChanged( true );
  }

  public String toString() {
    return name;
  }

  public boolean equals( Object obj ) {
    if ( obj == null || name == null ) {
      return false;
    }
    return name.equals( ( (PartitionSchema) obj ).name );
  }

  public int hashCode() {
    return name.hashCode();
  }

  public List<String> calculatePartitionIds() {
    int nrPartitions = Const.toInt( environmentSubstitute( numberOfPartitions ), -1 );
    if ( dynamicallyDefined ) {
      List<String> list = new ArrayList<>();
      for ( int i = 0; i < nrPartitions; i++ ) {
        list.add( "Partition-" + ( i + 1 ) );
      }
      return list;
    } else {
      return partitionIDs;
    }
  }

  public void copyVariablesFrom( IVariables variables ) {
    this.variables.copyVariablesFrom( variables );
  }

  public String environmentSubstitute( String aString ) {
    return variables.environmentSubstitute( aString );
  }

  public String[] environmentSubstitute( String[] aString ) {
    return variables.environmentSubstitute( aString );
  }

  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData )
    throws HopValueException {
    return variables.fieldSubstitute( aString, rowMeta, rowData );
  }

  public IVariables getParentVariableSpace() {
    return variables.getParentVariableSpace();
  }

  public void setParentVariableSpace( IVariables parent ) {
    variables.setParentVariableSpace( parent );
  }

  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaString.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  public void initializeVariablesFrom( IVariables parent ) {
    variables.initializeVariablesFrom( parent );
  }

  public String[] listVariables() {
    return variables.listVariables();
  }

  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  public void shareVariablesWith( IVariables variables ) {
    this.variables = variables;
  }

  public void injectVariables( Map<String, String> prop ) {
    variables.injectVariables( prop );
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * @return the partitionIDs
   */
  public List<String> getPartitionIDs() {
    return partitionIDs;
  }

  /**
   * @param partitionIDs the partitionIDs to set
   */
  public void setPartitionIDs( List<String> partitionIDs ) {
    this.partitionIDs = partitionIDs;
  }

  /**
   * @return the dynamicallyDefined
   */
  public boolean isDynamicallyDefined() {
    return dynamicallyDefined;
  }

  /**
   * @param dynamicallyDefined the dynamicallyDefined to set
   */
  public void setDynamicallyDefined( boolean dynamicallyDefined ) {
    this.dynamicallyDefined = dynamicallyDefined;
  }

  /**
   * @return the number of partitions
   */
  public String getNumberOfPartitions() {
    return numberOfPartitions;
  }

  /**
   * @param numberOfPartitions the number of partitions to set...
   */
  public void setNumberOfPartitions( String numberOfPartitions ) {
    this.numberOfPartitions = numberOfPartitions;
  }

}
