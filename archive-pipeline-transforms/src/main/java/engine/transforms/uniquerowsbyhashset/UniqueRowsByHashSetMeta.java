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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

public class UniqueRowsByHashSetMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = UniqueRowsByHashSetMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * Whether to compare strictly by hash value or to store the row values for strict equality checking
   */
  private boolean storeValues;

  /**
   * The fields to compare for duplicates, null means all
   */
  private String[] compareFields;

  private boolean rejectDuplicateRow;
  private String errorDescription;

  public UniqueRowsByHashSetMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @param compareField The compareField to set.
   */
  public void setCompareFields( String[] compareField ) {
    this.compareFields = compareField;
  }

  public boolean getStoreValues() {
    return storeValues;
  }

  public void setStoreValues( boolean storeValues ) {
    this.storeValues = storeValues;
  }

  /**
   * @return Returns the compareField.
   */
  public String[] getCompareFields() {
    return compareFields;
  }

  public void allocate( int nrFields ) {
    compareFields = new String[ nrFields ];
  }

  /**
   * @param rejectDuplicateRow The rejectDuplicateRow to set.
   */
  public void setRejectDuplicateRow( boolean rejectDuplicateRow ) {
    this.rejectDuplicateRow = rejectDuplicateRow;
  }

  /**
   * @return Returns the rejectDuplicateRow.
   */
  public boolean isRejectDuplicateRow() {
    return rejectDuplicateRow;
  }

  /**
   * @param errorDescription The errorDescription to set.
   */
  public void setErrorDescription( String errorDescription ) {
    this.errorDescription = errorDescription;
  }

  /**
   * @return Returns the errorDescription.
   */
  public String getErrorDescription() {
    return errorDescription;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    UniqueRowsByHashSetMeta retval = (UniqueRowsByHashSetMeta) super.clone();

    int nrFields = compareFields.length;

    retval.allocate( nrFields );

    System.arraycopy( compareFields, 0, retval.compareFields, 0, nrFields );
    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      storeValues = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "store_values" ) );
      rejectDuplicateRow = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "reject_duplicate_row" ) );
      errorDescription = XMLHandler.getTagValue( transformNode, "error_description" );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        compareFields[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "UniqueRowsByHashSetMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    rejectDuplicateRow = false;
    errorDescription = null;
    int nrFields = 0;

    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      compareFields[ i ] = "field" + i;
    }
  }

  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " + XMLHandler.addTagValue( "store_values", storeValues ) );
    retval.append( "      " + XMLHandler.addTagValue( "reject_duplicate_row", rejectDuplicateRow ) );
    retval.append( "      " + XMLHandler.addTagValue( "error_description", errorDescription ) );
    retval.append( "    <fields>" );
    for ( int i = 0; i < compareFields.length; i++ ) {
      retval.append( "      <field>" );
      retval.append( "        " + XMLHandler.addTagValue( "name", compareFields[ i ] ) );
      retval.append( "        </field>" );
    }
    retval.append( "      </fields>" );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "UniqueRowsByHashSetMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "UniqueRowsByHashSetMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new UniqueRowsByHashSet( transformMeta, transformDataInterface, cnr, pipelineMeta, pipeline );
  }

  public TransformDataInterface getTransformData() {
    return new UniqueRowsByHashSetData();
  }

  public boolean supportsErrorHandling() {
    return isRejectDuplicateRow();
  }
}
