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

package org.apache.hop.pipeline.transforms.mappinginput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

public class MappingInputMeta extends BaseTransformMeta implements ITransformMeta<MappingInput, MappingInputData> {

  private static Class<?> PKG = MappingInputMeta.class; // for i18n purposes, needed by Translator!!

  private String[] fieldName;

  private int[] fieldType;

  private int[] fieldLength;

  private int[] fieldPrecision;

  /**
   * Select: flag to indicate that the non-selected fields should also be taken along, ordered by fieldname
   */
  private boolean selectingAndSortingUnspecifiedFields;

  private volatile IRowMeta inputRowMeta;

  private volatile List<MappingValueRename> valueRenames;

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

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
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

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        fieldName[ i ] = XMLHandler.getTagValue( fnode, "name" );
        fieldType[ i ] = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "type" ) );
        String slength = XMLHandler.getTagValue( fnode, "length" );
        String sprecision = XMLHandler.getTagValue( fnode, "precision" );

        fieldLength[ i ] = Const.toInt( slength, -1 );
        fieldPrecision[ i ] = Const.toInt( sprecision, -1 );
      }

      selectingAndSortingUnspecifiedFields =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( fields, "select_unspecified" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "MappingInputMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      if ( fieldName[ i ] != null && fieldName[ i ].length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", fieldName[ i ] ) );
        retval
          .append( "        " ).append( XMLHandler.addTagValue( "type",
          ValueMetaFactory.getValueMetaName( fieldType[ i ] ) ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "length", fieldLength[ i ] ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "precision", fieldPrecision[ i ] ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }

    retval.append( "        " ).append(
      XMLHandler.addTagValue( "select_unspecified", selectingAndSortingUnspecifiedFields ) );

    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void setDefault() {
    int nrFields = 0;

    selectingAndSortingUnspecifiedFields = false;

    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      fieldName[ i ] = "field" + i;
      fieldType[ i ] = IValueMeta.TYPE_STRING;
      fieldLength[ i ] = 30;
      fieldPrecision[ i ] = -1;
    }
  }

  public void getFields( IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IMetaStore metaStore ) throws HopTransformException {
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
      // First rename any fields...
      if ( valueRenames != null ) {
        for ( MappingValueRename valueRename : valueRenames ) {
          IValueMeta valueMeta = inputRowMeta.searchValueMeta( valueRename.getSourceValueName() );
          if ( valueMeta == null ) {
            // ok, let's search once again, now using target name
            valueMeta = inputRowMeta.searchValueMeta( valueRename.getTargetValueName() );
            if ( valueMeta == null ) {
              throw new HopTransformException( BaseMessages.getString(
                PKG, "MappingInput.Exception.UnableToFindMappedValue", valueRename.getSourceValueName() ) );
            }
          } else {
            valueMeta.setName( valueRename.getTargetValueName() );
          }
        }
      }

      if ( selectingAndSortingUnspecifiedFields ) {
        // Select the specified fields from the input, re-order everything and put the other fields at the back,
        // sorted...
        //
        IRowMeta newRow = new RowMeta();

        for ( int i = 0; i < fieldName.length; i++ ) {
          int index = inputRowMeta.indexOfValue( fieldName[ i ] );
          if ( index < 0 ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "MappingInputMeta.Exception.UnknownField", fieldName[ i ] ) );
          }

          newRow.addValueMeta( inputRowMeta.getValueMeta( index ) );
        }

        // Now get the unspecified fields.
        // Sort the fields
        // Add them after the specified fields...
        //
        List<String> extra = new ArrayList<>();
        for ( int i = 0; i < inputRowMeta.size(); i++ ) {
          String fieldName = inputRowMeta.getValueMeta( i ).getName();
          if ( newRow.indexOfValue( fieldName ) < 0 ) {
            extra.add( fieldName );
          }
        }
        Collections.sort( extra );
        for ( String fieldName : extra ) {
          IValueMeta extraValue = inputRowMeta.searchValueMeta( fieldName );
          newRow.addValueMeta( extraValue );
        }

        // now merge the new row...
        // This is basically the input row meta data with the fields re-ordered.
        //
        row.mergeRowMeta( newRow );
      } else {
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
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.NotReceivingFieldsError" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.TransformReceivingDatasFromPreviousOne", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingInputMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform createTransform( TransformMeta transformMeta, MappingInputData data, int cnr, PipelineMeta tr,
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
   * @return the selectingAndSortingUnspecifiedFields
   */
  public boolean isSelectingAndSortingUnspecifiedFields() {
    return selectingAndSortingUnspecifiedFields;
  }

  /**
   * @param selectingAndSortingUnspecifiedFields the selectingAndSortingUnspecifiedFields to set
   */
  public void setSelectingAndSortingUnspecifiedFields( boolean selectingAndSortingUnspecifiedFields ) {
    this.selectingAndSortingUnspecifiedFields = selectingAndSortingUnspecifiedFields;
  }

}
