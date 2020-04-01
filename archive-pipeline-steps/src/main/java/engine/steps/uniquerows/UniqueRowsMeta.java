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

package org.apache.hop.pipeline.steps.uniquerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

public class UniqueRowsMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = UniqueRowsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * Indicate that we want to count the number of doubles
   */
  private boolean countRows;

  /**
   * The fieldname that will contain the number of doubles
   */
  private String countField;

  /**
   * The fields to compare for double, null means all
   */
  private String[] compareFields;

  /**
   * The fields to compare for double, null means all
   */
  private boolean[] caseInsensitive;

  private boolean rejectDuplicateRow;
  private String errorDescription;

  public UniqueRowsMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the countRows.
   */
  public boolean isCountRows() {
    return countRows;
  }

  /**
   * @param countRows The countRows to set.
   */
  public void setCountRows( boolean countRows ) {
    this.countRows = countRows;
  }

  /**
   * @return Returns the countField.
   */
  public String getCountField() {
    return countField;
  }

  /**
   * @param countField The countField to set.
   */
  public void setCountField( String countField ) {
    this.countField = countField;
  }

  /**
   * @param compareField The compareField to set.
   */
  public void setCompareFields( String[] compareField ) {
    this.compareFields = compareField;
  }

  /**
   * @return Returns the compareField.
   */
  public String[] getCompareFields() {
    return compareFields;
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

  public void allocate( int nrfields ) {
    compareFields = new String[ nrfields ];
    caseInsensitive = new boolean[ nrfields ];
  }

  /**
   * @return Returns the errorDescription.
   */
  public String getErrorDescription() {
    return errorDescription;
  }

  /**
   * @param errorDescription The errorDescription to set.
   */
  public void setErrorDescription( String errorDescription ) {
    this.errorDescription = errorDescription;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    UniqueRowsMeta retval = (UniqueRowsMeta) super.clone();

    int nrfields = compareFields.length;

    retval.allocate( nrfields );
    System.arraycopy( compareFields, 0, retval.compareFields, 0, nrfields );
    System.arraycopy( caseInsensitive, 0, retval.caseInsensitive, 0, nrfields );

    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      countRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "count_rows" ) );
      countField = XMLHandler.getTagValue( stepnode, "count_field" );
      rejectDuplicateRow = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "reject_duplicate_row" ) );
      errorDescription = XMLHandler.getTagValue( stepnode, "error_description" );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        compareFields[ i ] = XMLHandler.getTagValue( fnode, "name" );
        caseInsensitive[ i ] = !"N".equalsIgnoreCase( XMLHandler.getTagValue( fnode, "case_insensitive" ) );
      }

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "UniqueRowsMeta.Exception.UnableToLoadStepInfoFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    countRows = false;
    countField = "";
    rejectDuplicateRow = false;
    errorDescription = null;

    int nrfields = 0;

    allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      compareFields[ i ] = "field" + i;
      caseInsensitive[ i ] = true;
    }
  }

  @Override
  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // change the case insensitive flag too
    for ( int i = 0; i < compareFields.length; i++ ) {
      int idx = row.indexOfValue( compareFields[ i ] );
      if ( idx >= 0 ) {
        row.getValueMeta( idx ).setCaseInsensitive( caseInsensitive[ i ] );
      }
    }
    if ( countRows ) {
      ValueMetaInterface v = new ValueMetaInteger( countField );
      v.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "      " + XMLHandler.addTagValue( "count_rows", countRows ) );
    retval.append( "      " + XMLHandler.addTagValue( "count_field", countField ) );
    retval.append( "      " + XMLHandler.addTagValue( "reject_duplicate_row", rejectDuplicateRow ) );
    retval.append( "      " + XMLHandler.addTagValue( "error_description", errorDescription ) );

    retval.append( "    <fields>" );
    for ( int i = 0; i < compareFields.length; i++ ) {
      retval.append( "      <field>" );
      retval.append( "        " + XMLHandler.addTagValue( "name", compareFields[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "case_insensitive", caseInsensitive[ i ] ) );
      retval.append( "        </field>" );
    }
    retval.append( "      </fields>" );

    return retval.toString();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "UniqueRowsMeta.CheckResult.StepReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "UniqueRowsMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new UniqueRows( stepMeta, stepDataInterface, cnr, pipelineMeta, pipeline );
  }

  @Override
  public StepDataInterface getStepData() {
    return new UniqueRowsData();
  }

  /**
   * @return Returns the caseInsensitive.
   */
  public boolean[] getCaseInsensitive() {
    return caseInsensitive;
  }

  /**
   * @param caseInsensitive The caseInsensitive to set.
   */
  public void setCaseInsensitive( boolean[] caseInsensitive ) {
    this.caseInsensitive = caseInsensitive;
  }

  @Override
  public boolean supportsErrorHandling() {
    return isRejectDuplicateRow();
  }
}
