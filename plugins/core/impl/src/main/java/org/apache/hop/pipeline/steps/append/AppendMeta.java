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

package org.apache.hop.pipeline.steps.append;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.TransformationType;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepIOMeta;
import org.apache.hop.pipeline.step.StepIOMetaInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.step.errorhandling.Stream;
import org.apache.hop.pipeline.step.errorhandling.StreamIcon;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author Sven Boden
 * @since 3-june-2007
 */
@Step( id = "Append", i18nPackageName = "org.apache.hop.pipeline.steps.append",
  name = "Append.Name", description = "Append.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.step:BaseStep.Category.Flow" )
@InjectionSupported( localizationPrefix = "AppendMeta.Injection." )
public class AppendMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = Append.class; // for i18n purposes, needed by Translator!!

  @Injection( name = "HEAD_STEP" )
  public String headStepname;
  @Injection( name = "TAIL_STEP" )
  public String tailStepname;

  public AppendMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    AppendMeta retval = (AppendMeta) super.clone();

    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    retval.append( XMLHandler.addTagValue( "head_name", infoStreams.get( 0 ).getStepname() ) );
    retval.append( XMLHandler.addTagValue( "tail_name", infoStreams.get( 1 ).getStepname() ) );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
      StreamInterface headStream = infoStreams.get( 0 );
      StreamInterface tailStream = infoStreams.get( 1 );
      headStream.setSubject( XMLHandler.getTagValue( stepnode, "head_name" ) );
      tailStream.setSubject( XMLHandler.getTagValue( stepnode, "tail_name" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "AppendMeta.Exception.UnableToLoadStepInfo" ), e );
    }
  }

  public void setDefault() {
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    StepIOMetaInterface ioMeta = getStepIOMeta();
    List<StreamInterface> infoStreams = ioMeta.getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  public boolean chosesTargetSteps() {
    return false;
  }

  public String[] getTargetSteps() {
    return null;
  }

  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just take the info fields.
    //
    if ( info != null ) {
      if ( info.length > 0 && info[ 0 ] != null ) {
        r.mergeRowMeta( info[ 0 ] );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    StreamInterface headStream = infoStreams.get( 0 );
    StreamInterface tailStream = infoStreams.get( 1 );

    if ( headStream.getStepname() != null && tailStream.getStepname() != null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "AppendMeta.CheckResult.SourceStepsOK" ), stepMeta );
      remarks.add( cr );
    } else if ( headStream.getStepname() == null && tailStream.getStepname() == null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "AppendMeta.CheckResult.SourceStepsMissing" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "AppendMeta.CheckResult.OneSourceStepMissing" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new Append( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new AppendData();
  }

  /**
   * Returns the Input/Output metadata for this step.
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new StepIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "AppendMeta.InfoStream.FirstStream.Description" ), StreamIcon.INFO, null ) );
      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "AppendMeta.InfoStream.SecondStream.Description" ), StreamIcon.INFO, null ) );
      setStepIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetStepIoMeta() {
  }

  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, };
  }
}
