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

package org.apache.hop.pipeline.transforms.javafilter;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Objects;

/**
 * Contains the meta-data for the java filter transform: calculates conditions using Janino
 * <p>
 * Created on 30-oct-2009
 */
public class JavaFilterMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = JavaFilterMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * The formula calculations to be performed
   */
  private String condition;

  public JavaFilterMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getCondition() {
    return condition;
  }

  public void setCondition( String condition ) {
    this.condition = condition;
  }

  public void allocate( int nrCalcs ) {
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    List<StreamInterface> targetStreams = getTransformIOMeta().getTargetStreams();

    targetStreams.get( 0 ).setSubject( XMLHandler.getTagValue( transformNode, "send_true_to" ) );
    targetStreams.get( 1 ).setSubject( XMLHandler.getTagValue( transformNode, "send_false_to" ) );

    condition = XMLHandler.getTagValue( transformNode, "condition" );
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    List<StreamInterface> targetStreams = getTransformIOMeta().getTargetStreams();
    retval.append( XMLHandler.addTagValue( "send_true_to", targetStreams.get( 0 ).getTransformName() ) );
    retval.append( XMLHandler.addTagValue( "send_false_to", targetStreams.get( 1 ).getTransformName() ) );

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
    return Objects.hash( getTransformIOMeta().getTargetStreams(), condition );
  }

  public Object clone() {
    JavaFilterMeta retval = (JavaFilterMeta) super.clone();
    return retval;
  }

  public void setDefault() {
    condition = "true";
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    List<StreamInterface> targetStreams = getTransformIOMeta().getTargetStreams();
    for ( StreamInterface stream : targetStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    List<StreamInterface> targetStreams = getTransformIOMeta().getTargetStreams();

    if ( targetStreams.get( 0 ).getTransformName() != null && targetStreams.get( 1 ).getTransformName() != null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.BothTrueAndFalseTransformSpecified" ), transformMeta );
      remarks.add( cr );
    } else if ( targetStreams.get( 0 ).getTransformName() == null && targetStreams.get( 1 ).getTransformName() == null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.NeitherTrueAndFalseTransformSpecified" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.PlsSpecifyBothTrueAndFalseTransform" ), transformMeta );
      remarks.add( cr );
    }

    if ( targetStreams.get( 0 ).getTransformName() != null ) {
      int trueTargetIdx = Const.indexOfString( targetStreams.get( 0 ).getTransformName(), output );
      if ( trueTargetIdx < 0 ) {
        cr =
          new CheckResult(
            CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "JavaFilterMeta.CheckResult.TargetTransformInvalid", "true", targetStreams
              .get( 0 ).getTransformName() ), transformMeta );
        remarks.add( cr );
      }
    }

    if ( targetStreams.get( 1 ).getTransformName() != null ) {
      int falseTargetIdx = Const.indexOfString( targetStreams.get( 1 ).getTransformName(), output );
      if ( falseTargetIdx < 0 ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages
            .getString( PKG, "JavaFilterMeta.CheckResult.TargetTransformInvalid", "false", targetStreams
              .get( 1 ).getTransformName() ), transformMeta );
        remarks.add( cr );
      }
    }

    if ( Utils.isEmpty( condition ) ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.NoConditionSpecified" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.ConditionSpecified" ), transformMeta );
    }
    remarks.add( cr );

    // Look up fields in the input stream <prev>
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      // What fields are used in the condition?
      // TODO: verify condition, parse it
      //
    } else {
      error_message =
        BaseMessages.getString( PKG, "JavaFilterMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform" )
          + Const.CR;
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JavaFilterMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new JavaFilter( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new JavaFilterData();
  }

  /**
   * Returns the Input/Output metadata for this transform.
   */
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "JavaFilterMeta.InfoStream.True.Description" ), StreamIcon.TRUE, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "JavaFilterMeta.InfoStream.False.Description" ), StreamIcon.FALSE, null ) );
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }
}
