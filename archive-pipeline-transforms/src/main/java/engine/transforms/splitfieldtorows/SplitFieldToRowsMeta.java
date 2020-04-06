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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
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

public class SplitFieldToRowsMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = SplitFieldToRowsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * Field to split
   */
  private String splitField;

  /**
   * Split field based upon this delimiter.
   */
  private String delimiter;

  /**
   * New name of the split field
   */
  private String newFieldname;

  /**
   * Flag indicating that a row number field should be included in the output
   */
  private boolean includeRowNumber;

  /**
   * The name of the field in the output containing the row number
   */
  private String rowNumberField;

  /**
   * Flag indicating that we should reset RowNum for each file
   */
  private boolean resetRowNumber;

  /**
   * Flag indicating that the delimiter is a regular expression
   */
  private boolean isDelimiterRegex;

  public boolean isDelimiterRegex() {
    return isDelimiterRegex;
  }

  public void setDelimiterRegex( boolean isDelimiterRegex ) {
    this.isDelimiterRegex = isDelimiterRegex;
  }

  public SplitFieldToRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the delimiter.
   */
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * @param delimiter The delimiter to set.
   */
  public void setDelimiter( String delimiter ) {
    this.delimiter = delimiter;
  }

  /**
   * @return Returns the splitField.
   */
  public String getSplitField() {
    return splitField;
  }

  /**
   * @param splitField The splitField to set.
   */
  public void setSplitField( String splitField ) {
    this.splitField = splitField;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      splitField = XMLHandler.getTagValue( transformNode, "splitfield" );
      delimiter = XMLHandler.getTagValue( transformNode, "delimiter" );
      newFieldname = XMLHandler.getTagValue( transformNode, "newfield" );
      includeRowNumber = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "rownum" ) );
      resetRowNumber = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "resetrownumber" ) );
      rowNumberField = XMLHandler.getTagValue( transformNode, "rownum_field" );
      isDelimiterRegex = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "delimiter_is_regex" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "SplitFieldToRowsMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    splitField = "";
    delimiter = ";";
    newFieldname = "";
    includeRowNumber = false;
    isDelimiterRegex = false;
    rowNumberField = "";
    resetRowNumber = true;
  }

  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    IValueMeta v = new ValueMetaString( newFieldname );
    v.setOrigin( name );
    row.addValueMeta( v );

    // include row number
    if ( includeRowNumber ) {
      v = new ValueMetaInteger( variables.environmentSubstitute( rowNumberField ) );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "   " + XMLHandler.addTagValue( "splitfield", splitField ) );
    retval.append( "   " + XMLHandler.addTagValue( "delimiter", delimiter ) );
    retval.append( "   " + XMLHandler.addTagValue( "newfield", newFieldname ) );
    retval.append( "   " + XMLHandler.addTagValue( "rownum", includeRowNumber ) );
    retval.append( "   " + XMLHandler.addTagValue( "rownum_field", rowNumberField ) );
    retval.append( "   " + XMLHandler.addTagValue( "resetrownumber", resetRowNumber ) );
    retval.append( "   " + XMLHandler.addTagValue( "delimiter_is_regex", isDelimiterRegex ) );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    String error_message = "";
    CheckResult cr;

    // Look up fields in the input stream <prev>
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SplitFieldToRowsMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      error_message = "";

      IValueMeta v = prev.searchValueMeta( splitField );
      if ( v == null ) {
        error_message =
          BaseMessages.getString(
            PKG, "SplitFieldToRowsMeta.CheckResult.FieldToSplitNotPresentInInputStream", splitField );
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "SplitFieldToRowsMeta.CheckResult.FieldToSplitFoundInInputStream", splitField ), transformMeta );
        remarks.add( cr );
      }
    } else {
      error_message =
        BaseMessages.getString( PKG, "SplitFieldToRowsMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform" )
          + Const.CR;
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SplitFieldToRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransform" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SplitFieldToRowsMeta.CheckResult.NoInputReceivedFromOtherTransform" ), transformMeta );
      remarks.add( cr );
    }

    if ( Utils.isEmpty( newFieldname ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SplitFieldToRowsMeta.CheckResult.NewFieldNameIsNull" ), transformMeta );
      remarks.add( cr );
    }
    if ( includeRowNumber ) {
      if ( Utils.isEmpty( pipelineMeta.environmentSubstitute( rowNumberField ) ) ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "SplitFieldToRowsMeta.CheckResult.RowNumberFieldMissing" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "SplitFieldToRowsMeta.CheckResult.RowNumberFieldOk" ), transformMeta );
      }
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SplitFieldToRows( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new SplitFieldToRowsData();
  }

  /**
   * @return the newFieldname
   */
  public String getNewFieldname() {
    return newFieldname;
  }

  /**
   * @param newFieldname the newFieldname to set
   */
  public void setNewFieldname( String newFieldname ) {
    this.newFieldname = newFieldname;
  }

  /**
   * @return Returns the rowNumberField.
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /**
   * @param rowNumberField The rowNumberField to set.
   */
  public void setRowNumberField( String rowNumberField ) {
    this.rowNumberField = rowNumberField;
  }

  /**
   * @return Returns the resetRowNumber.
   */
  public boolean resetRowNumber() {
    return resetRowNumber;
  }

  /**
   * @param resetRowNumber The resetRowNumber to set.
   */
  public void setResetRowNumber( boolean resetRowNumber ) {
    this.resetRowNumber = resetRowNumber;
  }

  /**
   * @param includeRowNumber The includeRowNumber to set.
   */
  public void setIncludeRowNumber( boolean includeRowNumber ) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }
}
