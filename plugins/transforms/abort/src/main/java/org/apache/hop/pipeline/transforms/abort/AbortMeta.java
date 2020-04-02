/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.abort;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

import static org.apache.hop.core.util.StringUtil.isEmpty;

/**
 * Meta data for the abort transform.
 */
@Transform( id = "Abort", i18nPackageName = "org.apache.hop.pipeline.transforms.abort",
  name = "Abort.Name", description = "Abort.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow" )
public class AbortMeta extends BaseTransformMeta implements TransformMetaInterface<Abort, AbortData> {

  private static final Class<?> PKG = AbortMeta.class; // for i18n purposes, needed by Translator!!

  public enum AbortOption {
    ABORT,
    ABORT_WITH_ERROR,
    SAFE_STOP
  }

  /**
   * Threshold to abort.
   */
  private String rowThreshold;

  /**
   * Message to put in log when aborting.
   */
  private String message;

  /**
   * Always log rows.
   */
  private boolean alwaysLogRows;

  private AbortOption abortOption;

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    // Default: no values are added to the row in the transform
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transforminfo,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    // See if we have input streams leading to this transform!
    if ( input.length == 0 ) {
      CheckResult cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "AbortMeta.CheckResult.NoInputReceivedError" ), transforminfo );
      remarks.add( cr );
    }
  }

  @Override
  public Abort createTransform( TransformMeta transformMeta, AbortData transformDataInterface, int copyNr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Abort( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public AbortData getTransformData() {
    return new AbortData();
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  @Override
  public void setDefault() {
    rowThreshold = "0";
    message = "";
    alwaysLogRows = true;
    abortOption = AbortOption.ABORT;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "row_threshold", rowThreshold ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "message", message ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "always_log_rows", alwaysLogRows ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "abort_option", abortOption.toString() ) );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      rowThreshold = XMLHandler.getTagValue( transformNode, "row_threshold" );
      message = XMLHandler.getTagValue( transformNode, "message" );
      alwaysLogRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "always_log_rows" ) );
      String abortOptionString = XMLHandler.getTagValue( transformNode, "abort_option" );
      if ( !isEmpty( abortOptionString ) ) {
        abortOption = AbortOption.valueOf( abortOptionString );
      } else {
        // Backwards compatibility
        String awe = XMLHandler.getTagValue( transformNode, "abort_with_error" );
        if ( awe == null ) {
          awe = "Y"; // existing pipelines will have to maintain backward compatibility with yes
        }
        abortOption = "Y".equalsIgnoreCase( awe ) ? AbortOption.ABORT_WITH_ERROR : AbortOption.ABORT;
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "AbortMeta.Exception.UnexpectedErrorInReadingTransformMetaFromRepository" ), e ); // TODO: Change wording
    }
  }

  public String getMessage() {
    return message;
  }

  public void setMessage( String message ) {
    this.message = message;
  }

  public String getRowThreshold() {
    return rowThreshold;
  }

  public void setRowThreshold( String rowThreshold ) {
    this.rowThreshold = rowThreshold;
  }

  public boolean isAlwaysLogRows() {
    return alwaysLogRows;
  }

  public void setAlwaysLogRows( boolean alwaysLogRows ) {
    this.alwaysLogRows = alwaysLogRows;
  }

  public AbortOption getAbortOption() {
    return abortOption;
  }

  public void setAbortOption( AbortOption abortOption ) {
    this.abortOption = abortOption;
  }

  public boolean isAbortWithError() {
    return abortOption == AbortOption.ABORT_WITH_ERROR;
  }

  public boolean isAbort() {
    return abortOption == AbortOption.ABORT;
  }

  public boolean isSafeStop() {
    return abortOption == AbortOption.SAFE_STOP;
  }
}
