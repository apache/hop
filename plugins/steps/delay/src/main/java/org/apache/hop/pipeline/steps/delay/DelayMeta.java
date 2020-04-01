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

package org.apache.hop.pipeline.steps.delay;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.*;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 27-06-2008
 *
 */

@Step(
        id = "Delay",
        image = "ui/images/DLT.svg",
        i18nPackageName = "i18n:org.apache.hop.pipeline.steps.delay",
        name = "BaseStep.TypeLongDesc.Delay",
        description = "BaseStep.TypeTooltipDesc.Delay",
        categoryDescription = "i18n:org.apache.hop.pipeline.step:BaseStep.Category.Utility",
        documentationUrl = ""
)
public class DelayMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = DelayMeta.class; // for i18n purposes, needed by Translator!!

  private String timeout;
  private String scaletime;
  public static String DEFAULT_SCALE_TIME = "seconds";

  // before 3.1.1 it was "millisecond","second","minute","hour"-->
  // keep compatibility see PDI-1850, PDI-1532
  public String[] ScaleTimeCode = { "milliseconds", "seconds", "minutes", "hours" };

  public DelayMeta() {
    super(); // allocate BaseStepMeta
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "timeout", timeout ) );
    retval.append( "    " + XMLHandler.addTagValue( "scaletime", scaletime ) );

    return retval.toString();
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public String getScaleTime() {
    return scaletime;
  }

  public void setScaleTimeCode( int ScaleTimeIndex ) {
    switch ( ScaleTimeIndex ) {
      case 0:
        scaletime = ScaleTimeCode[ 0 ]; // milliseconds
        break;
      case 1:
        scaletime = ScaleTimeCode[ 1 ]; // second
        break;
      case 2:
        scaletime = ScaleTimeCode[ 2 ]; // minutes
        break;
      case 3:
        scaletime = ScaleTimeCode[ 3 ]; // hours
        break;
      default:
        scaletime = ScaleTimeCode[ 1 ]; // seconds
        break;
    }
  }

  public int getScaleTimeCode() {
    int retval = 1; // DEFAULT: seconds
    if ( scaletime == null ) {
      return retval;
    }
    if ( scaletime.equals( ScaleTimeCode[ 0 ] ) ) {
      retval = 0;
    } else if ( scaletime.equals( ScaleTimeCode[ 1 ] ) ) {
      retval = 1;
    } else if ( scaletime.equals( ScaleTimeCode[ 2 ] ) ) {
      retval = 2;
    } else if ( scaletime.equals( ScaleTimeCode[ 3 ] ) ) {
      retval = 3;
    }

    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      timeout = XMLHandler.getTagValue( stepnode, "timeout" );
      scaletime = XMLHandler.getTagValue( stepnode, "scaletime" );
      // set all unknown values to seconds
      setScaleTimeCode( getScaleTimeCode() ); // compatibility reasons for pipelines before 3.1.1, see PDI-1850,
      // PDI-1532

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "DelayMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  public void setDefault() {
    timeout = "1"; // default one second
    scaletime = DEFAULT_SCALE_TIME; // defaults to "seconds"
  }

  public String getTimeOut() {
    return timeout;
  }

  public void setTimeOut( String timeout ) {
    this.timeout = timeout;
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( timeout ) ) {
      error_message = BaseMessages.getString( PKG, "DelayMeta.CheckResult.TimeOutMissing" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
    } else {
      error_message = BaseMessages.getString( PKG, "DelayMeta.CheckResult.TimeOutOk" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta );
    }
    remarks.add( cr );

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.NotReceivingFields" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.StepRecevingData2" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
    }
    remarks.add( cr );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new Delay( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new DelayData();
  }

  @Override
  public String getDialogClassName(){
    return DelayDialog.class.getName();
  }
}
