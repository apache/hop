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

package org.apache.hop.pipeline.transforms.flattener;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/**
 * The flattener transform meta-data
 *
 * @author Matt
 * @since 17-jan-2006
 */

public class FlattenerMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = FlattenerMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * The field to flatten
   */
  private String fieldName;

  /**
   * Fields to flatten, same data type as input
   */
  private String[] targetField;

  public FlattenerMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  public String[] getTargetField() {
    return targetField;
  }

  public void setTargetField( String[] targetField ) {
    this.targetField = targetField;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocate( int nrFields ) {
    targetField = new String[ nrFields ];
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    int nrFields = 0;

    allocate( nrFields );
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    // Remove the key value (there will be different entries for each output row)
    //
    if ( fieldName != null && fieldName.length() > 0 ) {
      int idx = row.indexOfValue( fieldName );
      if ( idx < 0 ) {
        throw new HopTransformException( BaseMessages.getString(
          PKG, "FlattenerMeta.Exception.UnableToLocateFieldInInputFields", fieldName ) );
      }

      IValueMeta v = row.getValueMeta( idx );
      row.removeValueMeta( idx );

      for ( int i = 0; i < targetField.length; i++ ) {
        IValueMeta value = v.clone();
        value.setName( targetField[ i ] );
        value.setOrigin( name );

        row.addValueMeta( value );
      }
    } else {
      throw new HopTransformException( BaseMessages.getString( PKG, "FlattenerMeta.Exception.FlattenFieldRequired" ) );
    }
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      fieldName = XMLHandler.getTagValue( transformNode, "field_name" );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        targetField[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "FlattenerMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " + XMLHandler.addTagValue( "field_name", fieldName ) );

    retval.append( "      <fields>" + Const.CR );
    for ( int i = 0; i < targetField.length; i++ ) {
      retval.append( "        <field>" + Const.CR );
      retval.append( "          " + XMLHandler.addTagValue( "name", targetField[ i ] ) );
      retval.append( "          </field>" + Const.CR );
    }
    retval.append( "        </fields>" + Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FlattenerMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FlattenerMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Flattener( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new FlattenerData();
  }

}
