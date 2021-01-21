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

package org.apache.hop.pipeline.transforms.input;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
  id = "MappingInput",
  name = "i18n::BaseTransform.TypeLongDesc.MappingInput",
  description = "i18n:BaseTransform.TypeTooltipDesc.MappingInput",
  image = "MPI.svg",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Mapping"
)
public class MappingInputMeta extends BaseTransformMeta implements ITransformMeta<MappingInput, MappingInputData> {

  private static final Class<?> PKG = MappingInputMeta.class; // For Translator

  private String[] fieldName;

  private int[] fieldType;

  private int[] fieldLength;

  private int[] fieldPrecision;

  private volatile IRowMeta inputRowMeta;

  public MappingInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the fieldLength.
   */
  public int[] getFieldLength() {
    return fieldLength;
  }

  /**
   * @param fieldLength The fieldLength to set.
   */
  public void setFieldLength( int[] fieldLength ) {
    this.fieldLength = fieldLength;
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the fieldPrecision.
   */
  public int[] getFieldPrecision() {
    return fieldPrecision;
  }

  /**
   * @param fieldPrecision The fieldPrecision to set.
   */
  public void setFieldPrecision( int[] fieldPrecision ) {
    this.fieldPrecision = fieldPrecision;
  }

  /**
   * @return Returns the fieldType.
   */
  public int[] getFieldType() {
    return fieldType;
  }

  /**
   * @param fieldType The fieldType to set.
   */
  public void setFieldType( int[] fieldType ) {
    this.fieldType = fieldType;
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  public Object clone() {
    MappingInputMeta retval = (MappingInputMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate( nrFields );

    System.arraycopy( fieldName, 0, retval.fieldName, 0, nrFields );
    System.arraycopy( fieldType, 0, retval.fieldType, 0, nrFields );
    System.arraycopy( fieldLength, 0, retval.fieldLength, 0, nrFields );
    System.arraycopy( fieldPrecision, 0, retval.fieldPrecision, 0, nrFields );
    return retval;
  }

  public void allocate( int nrFields ) {
    fieldName = new String[ nrFields ];
    fieldType = new int[ nrFields ];
    fieldLength = new int[ nrFields ];
    fieldPrecision = new int[ nrFields ];
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode( transformNode, "fields" );
      int nrFields = XmlHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        fieldName[ i ] = XmlHandler.getTagValue( fnode, "name" );
        fieldType[ i ] = ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( fnode, "type" ) );
        fieldLength[ i ] = Const.toInt( XmlHandler.getTagValue( fnode, "length" ), -1 );
        fieldPrecision[ i ] = Const.toInt( XmlHandler.getTagValue( fnode, "precision" ), -1 );
      }
    } catch ( Exception e ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "MappingInputMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder( 300 );

    xml.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      if ( fieldName[ i ] != null && fieldName[ i ].length() != 0 ) {
        xml.append( "      <field>" ).append( Const.CR );
        xml.append( "        " ).append( XmlHandler.addTagValue( "name", fieldName[ i ] ) );
        xml.append( "        " ).append( XmlHandler.addTagValue( "type", ValueMetaFactory.getValueMetaName( fieldType[ i ] ) ) );
        xml.append( "        " ).append( XmlHandler.addTagValue( "length", fieldLength[ i ] ) );
        xml.append( "        " ).append( XmlHandler.addTagValue( "precision", fieldPrecision[ i ] ) );
        xml.append( "      </field>" ).append( Const.CR );
      }
    }

    xml.append( "    </fields>" ).append( Const.CR );

    return xml.toString();
  }

  public void setDefault() {
    int nrFields = 0;

    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      fieldName[ i ] = "field" + i;
      fieldType[ i ] = IValueMeta.TYPE_STRING;
      fieldLength[ i ] = 30;
      fieldPrecision[ i ] = -1;
    }
  }

  public void getFields( IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // Row should normally be empty when we get here.
    // That is because there is no previous transform to this mapping input transform from the viewpoint of this single
    // sub-pipeline.
    // From the viewpoint of the pipeline that executes the mapping, it's important to know what comes out at the
    // exit points.
    // For that reason we need to re-order etc, based on the input specification...
    //
    if ( inputRowMeta != null && !inputRowMeta.isEmpty() ) {
      // this gets set only in the parent pipeline...
      // It includes all the renames that needed to be done
      //
      row.mergeRowMeta( inputRowMeta );

      // Validate the existence of all the specified fields...
      //
      if ( !row.isEmpty() ) {
        for ( int i = 0; i < fieldName.length; i++ ) {
          if ( row.indexOfValue( fieldName[ i ] ) < 0 ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "MappingInputMeta.Exception.UnknownField", fieldName[ i ] ) );
          }
        }
      }
    } else {
      if ( row.isEmpty() ) {
        // We'll have to work with the statically provided information
        for ( int i = 0; i < fieldName.length; i++ ) {
          if ( !Utils.isEmpty( fieldName[ i ] ) ) {
            int valueType = fieldType[ i ];
            if ( valueType == IValueMeta.TYPE_NONE ) {
              valueType = IValueMeta.TYPE_STRING;
            }
            IValueMeta v;
            try {
              v = ValueMetaFactory.createValueMeta( fieldName[ i ], valueType );
              v.setLength( fieldLength[ i ] );
              v.setPrecision( fieldPrecision[ i ] );
              v.setOrigin( origin );
              row.addValueMeta( v );
            } catch ( HopPluginException e ) {
              throw new HopTransformException( e );
            }
          }
        }
      }

      // else: row is OK, keep it as it is.
    }
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.NotReceivingFieldsError" ), transformMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.TransformReceivingDatasFromPreviousOne", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr = new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public MappingInput createTransform( TransformMeta transformMeta, MappingInputData data, int cnr, PipelineMeta tr,
                                     Pipeline pipeline ) {
    return new MappingInput( transformMeta, this, data, cnr, tr, pipeline );
  }

  public MappingInputData getTransformData() {
    return new MappingInputData();
  }

  public void setInputRowMeta( IRowMeta inputRowMeta ) {
    this.inputRowMeta = inputRowMeta;
  }

  /**
   * @return the inputRowMeta
   */
  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }
}
