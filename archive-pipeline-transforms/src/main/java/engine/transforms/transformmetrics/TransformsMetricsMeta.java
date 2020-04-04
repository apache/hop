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

package org.apache.hop.pipeline.transforms.transformsmetrics;

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
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 30-06-2008
 *
 */

public class TransformsMetricsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = TransformsMetrics.class; // for i18n purposes, needed by Translator!!

  public static final String[] RequiredTransformsDesc = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };
  public static final String[] RequiredTransformsCode = new String[] { "N", "Y" };

  public static final String YES = "Y";
  public static final String NO = "N";

  /**
   * by which transforms to display?
   */
  private String[] transformName;
  private String[] transformCopyNr;
  /**
   * Array of boolean values as string, indicating if a transform is required.
   */
  private String[] transformRequired;

  private String transformnamefield;
  private String transformidfield;
  private String transformlinesinputfield;
  private String transformlinesoutputfield;
  private String transformlinesreadfield;
  private String transformlinesupdatedfield;
  private String transformlineswrittentfield;
  private String transformlineserrorsfield;
  private String transformsecondsfield;

  public TransformsMetricsMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    TransformsMetricsMeta retval = (TransformsMetricsMeta) super.clone();

    int nrFields = transformName.length;

    retval.allocate( nrFields );

    System.arraycopy( transformName, 0, retval.transformName, 0, nrFields );
    System.arraycopy( transformCopyNr, 0, retval.transformCopyNr, 0, nrFields );
    System.arraycopy( transformRequired, 0, retval.transformRequired, 0, nrFields );
    return retval;
  }

  public void allocate( int nrFields ) {
    transformName = new String[ nrFields ];
    transformCopyNr = new String[ nrFields ];
    transformRequired = new String[ nrFields ];
  }

  /**
   * @return Returns the transformName.
   */
  public String[] getTransformName() {
    return transformName;
  }

  /**
   * @return Returns the transformCopyNr.
   */
  public String[] getTransformCopyNr() {
    return transformCopyNr;
  }

  /**
   * @param transformName The transformName to set.
   */
  public void setTransformName( String[] transformName ) {
    this.transformName = transformName;
  }

  /**
   * @param transformCopyNr The transformCopyNr to set.
   */
  public void setTransformCopyNr( String[] transformCopyNr ) {
    this.transformCopyNr = transformCopyNr;
  }

  public String getRequiredTransformsDesc( String tt ) {
    if ( tt == null ) {
      return RequiredTransformsDesc[ 0 ];
    }
    if ( tt.equals( RequiredTransformsCode[ 1 ] ) ) {
      return RequiredTransformsDesc[ 1 ];
    } else {
      return RequiredTransformsDesc[ 0 ];
    }
  }

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    r.clear();
    String transformName = variables.environmentSubstitute( transformnamefield );
    if ( !Utils.isEmpty( transformName ) ) {
      IValueMeta v = new ValueMetaString( transformName );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    String transformid = variables.environmentSubstitute( transformidfield );
    if ( !Utils.isEmpty( transformid ) ) {
      IValueMeta v = new ValueMetaString( transformid );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformlinesinput = variables.environmentSubstitute( transformlinesinputfield );
    if ( !Utils.isEmpty( transformlinesinput ) ) {
      IValueMeta v = new ValueMetaInteger( transformlinesinput );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformlinesoutput = variables.environmentSubstitute( transformlinesoutputfield );
    if ( !Utils.isEmpty( transformlinesoutput ) ) {
      IValueMeta v = new ValueMetaInteger( transformlinesoutput );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformlinesread = variables.environmentSubstitute( transformlinesreadfield );
    if ( !Utils.isEmpty( transformlinesread ) ) {
      IValueMeta v = new ValueMetaInteger( transformlinesread );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformlinesupdated = variables.environmentSubstitute( transformlinesupdatedfield );
    if ( !Utils.isEmpty( transformlinesupdated ) ) {
      IValueMeta v = new ValueMetaInteger( transformlinesupdated );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformlineswritten = variables.environmentSubstitute( transformlineswrittentfield );
    if ( !Utils.isEmpty( transformlineswritten ) ) {
      IValueMeta v = new ValueMetaInteger( transformlineswritten );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformlineserrors = variables.environmentSubstitute( transformlineserrorsfield );
    if ( !Utils.isEmpty( transformlineserrors ) ) {
      IValueMeta v = new ValueMetaInteger( transformlineserrors );
      v.setOrigin( name );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      r.addValueMeta( v );
    }
    String transformseconds = variables.environmentSubstitute( transformsecondsfield );
    if ( !Utils.isEmpty( transformseconds ) ) {
      IValueMeta v = new ValueMetaInteger( transformseconds );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      Node transforms = XMLHandler.getSubNode( transformNode, "transforms" );
      int nrTransforms = XMLHandler.countNodes( transforms, "transform" );

      allocate( nrTransforms );

      for ( int i = 0; i < nrTransforms; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( transforms, "transform", i );
        transformName[ i ] = XMLHandler.getTagValue( fnode, "name" );
        transformCopyNr[ i ] = XMLHandler.getTagValue( fnode, "copyNr" );
        transformRequired[ i ] = XMLHandler.getTagValue( fnode, "transformRequired" );
      }
      transformnamefield = XMLHandler.getTagValue( transformNode, "transformnamefield" );
      transformidfield = XMLHandler.getTagValue( transformNode, "transformidfield" );
      transformlinesinputfield = XMLHandler.getTagValue( transformNode, "transformlinesinputfield" );
      transformlinesoutputfield = XMLHandler.getTagValue( transformNode, "transformlinesoutputfield" );
      transformlinesreadfield = XMLHandler.getTagValue( transformNode, "transformlinesreadfield" );
      transformlinesupdatedfield = XMLHandler.getTagValue( transformNode, "transformlinesupdatedfield" );
      transformlineswrittentfield = XMLHandler.getTagValue( transformNode, "transformlineswrittentfield" );
      transformlineserrorsfield = XMLHandler.getTagValue( transformNode, "transformlineserrorsfield" );
      transformsecondsfield = XMLHandler.getTagValue( transformNode, "transformsecondsfield" );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <transforms>" + Const.CR );
    for ( int i = 0; i < transformName.length; i++ ) {
      retval.append( "      <transform>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", transformName[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "copyNr", transformCopyNr[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "transformRequired", transformRequired[ i ] ) );
      retval.append( "        </transform>" + Const.CR );
    }
    retval.append( "      </transforms>" + Const.CR );

    retval.append( "        " + XMLHandler.addTagValue( "transformnamefield", transformnamefield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformidfield", transformidfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformlinesinputfield", transformlinesinputfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformlinesoutputfield", transformlinesoutputfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformlinesreadfield", transformlinesreadfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformlinesupdatedfield", transformlinesupdatedfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformlineswrittentfield", transformlineswrittentfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformlineserrorsfield", transformlineserrorsfield ) );
    retval.append( "        " + XMLHandler.addTagValue( "transformsecondsfield", transformsecondsfield ) );

    return retval.toString();
  }

  public void setDefault() {
    int nrTransforms = 0;

    allocate( nrTransforms );

    for ( int i = 0; i < nrTransforms; i++ ) {
      transformName[ i ] = "transform" + i;
      transformCopyNr[ i ] = "CopyNr" + i;
      transformRequired[ i ] = NO;
    }

    transformnamefield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.TransformName" );
    transformidfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Transformid" );
    transformlinesinputfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Linesinput" );
    transformlinesoutputfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Linesoutput" );
    transformlinesreadfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Linesread" );
    transformlinesupdatedfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Linesupdated" );
    transformlineswrittentfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Lineswritten" );
    transformlineserrorsfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Lineserrors" );
    transformsecondsfield = BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.Time" );
  }

  public void setTransformRequired( String[] transformRequiredin ) {
    for ( int i = 0; i < transformRequiredin.length; i++ ) {
      this.transformRequired[ i ] = getRequiredTransformsCode( transformRequiredin[ i ] );
    }
  }

  public String getRequiredTransformsCode( String tt ) {
    if ( tt == null ) {
      return RequiredTransformsCode[ 0 ];
    }
    if ( tt.equals( RequiredTransformsDesc[ 1 ] ) ) {
      return RequiredTransformsCode[ 1 ];
    } else {
      return RequiredTransformsCode[ 0 ];
    }
  }

  public String[] getTransformRequired() {
    return transformRequired;
  }

  public String getTransformNameFieldName() {
    return this.transformnamefield;
  }

  public void setTransformNameFieldName( String transformnamefield ) {
    this.transformnamefield = transformnamefield;
  }

  public String getTransformIdFieldName() {
    return this.transformidfield;
  }

  public void setTransformIdFieldName( String transformidfield ) {
    this.transformidfield = transformidfield;
  }

  public String getTransformLinesInputFieldName() {
    return this.transformlinesinputfield;
  }

  public void setTransformLinesInputFieldName( String transformlinesinputfield ) {
    this.transformlinesinputfield = transformlinesinputfield;
  }

  public String getTransformLinesOutputFieldName() {
    return this.transformlinesoutputfield;
  }

  public void setTransformLinesOutputFieldName( String transformlinesoutputfield ) {
    this.transformlinesoutputfield = transformlinesoutputfield;
  }

  public String getTransformLinesReadFieldName() {
    return this.transformlinesreadfield;
  }

  public void setTransformLinesReadFieldName( String transformlinesreadfield ) {
    this.transformlinesreadfield = transformlinesreadfield;
  }

  public String getTransformLinesWrittenFieldName() {
    return this.transformlineswrittentfield;
  }

  public void setTransformLinesWrittenFieldName( String transformlineswrittentfield ) {
    this.transformlineswrittentfield = transformlineswrittentfield;
  }

  public String getTransformLinesErrorsFieldName() {
    return this.transformlineserrorsfield;
  }

  public String getTransformSecondsFieldName() {
    return this.transformsecondsfield;
  }

  public void setTransformSecondsFieldName( String fieldname ) {
    this.transformsecondsfield = fieldname;
  }

  public void setTransformLinesErrorsFieldName( String transformlineserrorsfield ) {
    this.transformlineserrorsfield = transformlineserrorsfield;
  }

  public String getTransformLinesUpdatedFieldName() {
    return this.transformlinesupdatedfield;
  }

  public void setTransformLinesUpdatedFieldName( String transformlinesupdatedfield ) {
    this.transformlinesupdatedfield = transformlinesupdatedfield;
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TransformsMetricsMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
      if ( transformName.length > 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "TransformsMetricsMeta.CheckResult.AllTransformsFound" ), transformMeta );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "TransformsMetricsMeta.CheckResult.NoTransformsEntered" ), transformMeta );
      }
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "TransformsMetricsMeta.CheckResult.ReceivingFields" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TransformsMetricsMeta.CheckResult.TransformRecevingData2" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "TransformsMetricsMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
    }
    remarks.add( cr );

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new TransformsMetrics( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new TransformsMetricsData();
  }

  @Override
  public PipelineMeta.PipelineType[] getSupportedPipelineTypes() {
    return new PipelineMeta.PipelineType[] { PipelineMeta.PipelineType.Normal };
  }
}
