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

package org.apache.hop.pipeline;

import org.apache.hop.core.annotations.PartitionerPlugin;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

@PartitionerPlugin (
  id = "ModPartitioner",
  name = "Remainder of division",
  description = "Mod"
)   
public class ModPartitioner extends BasePartitioner implements IXml {

  private String fieldName;
  protected int partitionColumnIndex = -1;

  public ModPartitioner() {
    super();
  }

  public IPartitioner getInstance() {
    IPartitioner partitioner = new ModPartitioner();
    partitioner.setId( getId() );
    partitioner.setDescription( getDescription() );
    return partitioner;
  }

  public ModPartitioner clone() {
    ModPartitioner modPartitioner = (ModPartitioner) super.clone();
    modPartitioner.fieldName = fieldName;

    return modPartitioner;
  }

  public String getDialogClassName() {
    return "org.apache.hop.ui.pipeline.dialog.ModPartitionerDialog";
  }

  public int getPartition( IVariables variables, IRowMeta rowMeta, Object[] row ) throws HopException {

    if (rowMeta==null) {
      throw new HopException( "No row metadata was provided and so a partition can't be calculated on field '"+fieldName+"' using a mod partitioner" );
    }

    init( variables, rowMeta );

    if ( partitionColumnIndex < 0 ) {
      partitionColumnIndex = rowMeta.indexOfValue( fieldName );
      if ( partitionColumnIndex < 0 ) {
        throw new HopTransformException( "Unable to find partitioning field name [" + fieldName + "] in the output row..." + rowMeta );
      }
    }

    long value;

    IValueMeta valueMeta = rowMeta.getValueMeta( partitionColumnIndex );
    Object valueData = row[ partitionColumnIndex ];

    switch ( valueMeta.getType() ) {
      case IValueMeta.TYPE_INTEGER:
        Long longValue = rowMeta.getInteger( row, partitionColumnIndex );
        if ( longValue == null ) {
          value = valueMeta.hashCode( valueData );
        } else {
          value = longValue.longValue();
        }
        break;
      default:
        value = valueMeta.hashCode( valueData );
    }

    /*
     * value = rowMeta.getInteger(row, partitionColumnIndex);
     */

    int targetLocation = (int) ( Math.abs( value ) % nrPartitions );

    return targetLocation;
  }

  public String getDescription() {
    String description = "Mod partitioner";
    if ( !Utils.isEmpty( fieldName ) ) {
      description += "(" + fieldName + ")";
    }
    return description;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder( 150 );
    xml.append( "           " ).append( XmlHandler.addTagValue( "field_name", fieldName ) );
    return xml.toString();
  }

  public void loadXml( Node partitioningMethodNode ) throws HopXmlException {
    fieldName = XmlHandler.getTagValue( partitioningMethodNode, "field_name" );
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

}
