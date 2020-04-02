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

package org.apache.hop.pipeline.transforms.samplerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
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

/*
 * Created on 02-jun-2008
 *
 */

public class SampleRowsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = SampleRowsMeta.class; // for i18n purposes, needed by Translator!!

  private String linesrange;
  private String linenumfield;
  public static String DEFAULT_RANGE = "1";

  public SampleRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    if ( !Utils.isEmpty( linenumfield ) ) {

      ValueMetaInterface v = new ValueMetaInteger( space.environmentSubstitute( linenumfield ) );
      v.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      linesrange = XMLHandler.getTagValue( transformNode, "linesrange" );
      linenumfield = XMLHandler.getTagValue( transformNode, "linenumfield" );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "SampleRowsMeta.Exception.UnableToReadTransformMeta" ), e );
    }
  }

  public String getLinesRange() {
    return this.linesrange;
  }

  public void setLinesRange( String linesrange ) {
    this.linesrange = linesrange;
  }

  public String getLineNumberField() {
    return this.linenumfield;
  }

  public void setLineNumberField( String linenumfield ) {
    this.linenumfield = linenumfield;
  }

  public void setDefault() {
    linesrange = DEFAULT_RANGE;
    linenumfield = null;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "linesrange", linesrange ) );
    retval.append( "    " + XMLHandler.addTagValue( "linenumfield", linenumfield ) );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( Utils.isEmpty( linesrange ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SampleRowsMeta.CheckResult.LinesRangeMissing" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SampleRowsMeta.CheckResult.LinesRangeOk" ), transformMeta );
    }
    remarks.add( cr );

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SampleRowsMeta.CheckResult.NotReceivingFields" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SampleRowsMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SampleRowsMeta.CheckResult.TransformRecevingData2" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SampleRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
    }
    remarks.add( cr );
  }

  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new SampleRows( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }

  public TransformDataInterface getTransformData() {
    return new SampleRowsData();
  }

}
