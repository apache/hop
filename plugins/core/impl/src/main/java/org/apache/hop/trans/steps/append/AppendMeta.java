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

package org.apache.hop.trans.steps.append;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransMeta.TransformationType;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepIOMeta;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.step.errorhandling.Stream;
import org.apache.hop.trans.step.errorhandling.StreamIcon;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.step.errorhandling.StreamInterface.StreamType;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author Sven Boden
 * @since 3-june-2007
 */
@Step( id = "Append", i18nPackageName = "org.apache.hop.trans.steps.append",
  name = "Append.Name", description = "Append.Description",
  categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Flow" )
@InjectionSupported( localizationPrefix = "AppendMeta.Injection." )
public class AppendMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = Append.class; // for i18n purposes, needed by Translator2!!

  @Injection( name = "HEAD_STEP" )
  public String headStepname;
  @Injection( name = "TAIL_STEP" )
  public String tailStepname;

  public AppendMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws HopXMLException {
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

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws HopException {
    try {
      List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
      StreamInterface headStream = infoStreams.get( 0 );
      StreamInterface tailStream = infoStreams.get( 1 );
      headStream.setSubject( rep.getStepAttributeString( id_step, "head_name" ) );
      tailStream.setSubject( rep.getStepAttributeString( id_step, "tail_name" ) );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "AppendMeta.Exception.UnexpectedErrorReadingStepInfo" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws HopException {
    try {
      List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
      StreamInterface headStream = infoStreams.get( 0 );
      StreamInterface tailStream = infoStreams.get( 1 );
      rep.saveStepAttribute( id_transformation, id_step, "head_name", headStream.getStepname() );
      rep.saveStepAttribute( id_transformation, id_step, "tail_name", tailStream.getStepname() );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "AppendMeta.Exception.UnableToSaveStepInfo" )
        + id_step, e );
    }
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
    VariableSpace space, Repository repository, IMetaStore metaStore ) throws HopStepException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just take the info fields.
    //
    if ( info != null ) {
      if ( info.length > 0 && info[0] != null ) {
        r.mergeRowMeta( info[0] );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
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

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
    Trans trans ) {
    return new Append( stepMeta, stepDataInterface, cnr, tr, trans );
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
