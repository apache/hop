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

package org.apache.hop.pipeline.transforms.append;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author Sven Boden
 * @since 3-june-2007
 */
@Transform( id = "Append", i18nPackageName = "org.apache.hop.pipeline.transforms.append",
  name = "Append.Name", description = "Append.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow" )
@InjectionSupported( localizationPrefix = "AppendMeta.Injection." )
public class AppendMeta extends BaseTransformMeta implements TransformMetaInterface<Append, AppendData> {

  private static final Class<?> PKG = Append.class; // for i18n purposes, needed by Translator!!

  @Injection( name = "HEAD_TRANSFORM" )
  public String headTransformName;
  @Injection( name = "TAIL_TRANSFORM" )
  public String tailTransformName;

  public AppendMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    AppendMeta retval = (AppendMeta) super.clone();

    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    retval.append( XMLHandler.addTagValue( "head_name", infoStreams.get( 0 ).getTransformName() ) );
    retval.append( XMLHandler.addTagValue( "tail_name", infoStreams.get( 1 ).getTransformName() ) );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
      StreamInterface headStream = infoStreams.get( 0 );
      StreamInterface tailStream = infoStreams.get( 1 );
      headStream.setSubject( XMLHandler.getTagValue( transformNode, "head_name" ) );
      tailStream.setSubject( XMLHandler.getTagValue( transformNode, "tail_name" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "AppendMeta.Exception.UnableToLoadTransformMeta" ), e );
    }
  }

  public void setDefault() {
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    TransformIOMetaInterface ioMeta = getTransformIOMeta();
    List<StreamInterface> infoStreams = ioMeta.getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
    }
  }

  public boolean chosesTargetTransforms() {
    return false;
  }

  public String[] getTargetTransforms() {
    return null;
  }

  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just take the info fields.
    //
    if ( info != null ) {
      if ( info.length > 0 && info[ 0 ] != null ) {
        r.mergeRowMeta( info[ 0 ] );
      }
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    StreamInterface headStream = infoStreams.get( 0 );
    StreamInterface tailStream = infoStreams.get( 1 );

    if ( headStream.getTransformName() != null && tailStream.getTransformName() != null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "AppendMeta.CheckResult.SourceTransformsOK" ), transformMeta );
      remarks.add( cr );
    } else if ( headStream.getTransformName() == null && tailStream.getTransformName() == null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "AppendMeta.CheckResult.SourceTransformsMissing" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "AppendMeta.CheckResult.OneSourceTransformMissing" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public Append createTransform( TransformMeta transformMeta, AppendData transformDataInterface, int cnr, PipelineMeta tr,
                                 Pipeline pipeline ) {
    return new Append( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }

  @Override
  public AppendData getTransformData() {
    return new AppendData();
  }

  /**
   * Returns the Input/Output metadata for this transform.
   */
  @Override
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "AppendMeta.InfoStream.FirstStream.Description" ), StreamIcon.INFO, null ) );
      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "AppendMeta.InfoStream.SecondStream.Description" ), StreamIcon.INFO, null ) );
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }
}
