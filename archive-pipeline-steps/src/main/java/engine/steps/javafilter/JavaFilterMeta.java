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

package org.apache.hop.pipeline.steps.javafilter;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
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
import java.util.Objects;

/**
 * Contains the meta-data for the java filter step: calculates conditions using Janino
 * <p>
 * Created on 30-oct-2009
 */
public class JavaFilterMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = JavaFilterMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * The formula calculations to be performed
   */
  private String condition;

  public JavaFilterMeta() {
    super(); // allocate BaseStepMeta
  }

  public String getCondition() {
    return condition;
  }

  public void setCondition( String condition ) {
    this.condition = condition;
  }

  public void allocate( int nrCalcs ) {
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();

    targetStreams.get( 0 ).setSubject( XMLHandler.getTagValue( stepnode, "send_true_to" ) );
    targetStreams.get( 1 ).setSubject( XMLHandler.getTagValue( stepnode, "send_false_to" ) );

    condition = XMLHandler.getTagValue( stepnode, "condition" );
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();
    retval.append( XMLHandler.addTagValue( "send_true_to", targetStreams.get( 0 ).getStepname() ) );
    retval.append( XMLHandler.addTagValue( "send_false_to", targetStreams.get( 1 ).getStepname() ) );

    retval.append( XMLHandler.addTagValue( "condition", condition ) );

    return retval.toString();
  }

  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      JavaFilterMeta m = (JavaFilterMeta) obj;
      return ( getXML() == m.getXML() );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash( getStepIOMeta().getTargetStreams(), condition );
  }

  public Object clone() {
    JavaFilterMeta retval = (JavaFilterMeta) super.clone();
    return retval;
  }

  public void setDefault() {
    condition = "true";
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();
    for ( StreamInterface stream : targetStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();

    if ( targetStreams.get( 0 ).getStepname() != null && targetStreams.get( 1 ).getStepname() != null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.BothTrueAndFalseStepSpecified" ), stepMeta );
      remarks.add( cr );
    } else if ( targetStreams.get( 0 ).getStepname() == null && targetStreams.get( 1 ).getStepname() == null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.NeitherTrueAndFalseStepSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.PlsSpecifyBothTrueAndFalseStep" ), stepMeta );
      remarks.add( cr );
    }

    if ( targetStreams.get( 0 ).getStepname() != null ) {
      int trueTargetIdx = Const.indexOfString( targetStreams.get( 0 ).getStepname(), output );
      if ( trueTargetIdx < 0 ) {
        cr =
          new CheckResult(
            CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "JavaFilterMeta.CheckResult.TargetStepInvalid", "true", targetStreams
              .get( 0 ).getStepname() ), stepMeta );
        remarks.add( cr );
      }
    }

    if ( targetStreams.get( 1 ).getStepname() != null ) {
      int falseTargetIdx = Const.indexOfString( targetStreams.get( 1 ).getStepname(), output );
      if ( falseTargetIdx < 0 ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages
            .getString( PKG, "JavaFilterMeta.CheckResult.TargetStepInvalid", "false", targetStreams
              .get( 1 ).getStepname() ), stepMeta );
        remarks.add( cr );
      }
    }

    if ( Utils.isEmpty( condition ) ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.NoConditionSpecified" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.ConditionSpecified" ), stepMeta );
    }
    remarks.add( cr );

    // Look up fields in the input stream <prev>
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.StepReceivingFields", prev.size() + "" ), stepMeta );
      remarks.add( cr );

      // What fields are used in the condition?
      // TODO: verify condition, parse it
      //
    } else {
      error_message =
        BaseMessages.getString( PKG, "JavaFilterMeta.CheckResult.CouldNotReadFieldsFromPreviousStep" )
          + Const.CR;
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.StepReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new JavaFilter( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new JavaFilterData();
  }

  /**
   * Returns the Input/Output metadata for this step.
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new StepIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "JavaFilterMeta.InfoStream.True.Description" ), StreamIcon.TRUE, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "JavaFilterMeta.InfoStream.False.Description" ), StreamIcon.FALSE, null ) );
      setStepIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetStepIoMeta() {
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }
}
