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

package org.apache.hop.pipeline.transforms.delay;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
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
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 27-06-2008
 *
 */

public class DelayMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = DelayMeta.class; // for i18n purposes, needed by Translator!!

  private String timeout;
  private String scaletime;
  public static String DEFAULT_SCALE_TIME = "seconds";

  // before 3.1.1 it was "millisecond","second","minute","hour"-->
  // keep compatibility see PDI-1850, PDI-1532
  public String[] ScaleTimeCode = { "milliseconds", "seconds", "minutes", "hours" };

  public DelayMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " + XMLHandler.addTagValue( "timeout", timeout ) );
    retval.append( "    " + XMLHandler.addTagValue( "scaletime", scaletime ) );

    return retval.toString();
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
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

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      timeout = XMLHandler.getTagValue( transformNode, "timeout" );
      scaletime = XMLHandler.getTagValue( transformNode, "scaletime" );
      // set all unknown values to seconds
      setScaleTimeCode( getScaleTimeCode() ); // compatibility reasons for pipelines before 3.1.1, see PDI-1850,
      // PDI-1532

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "DelayMeta.Exception.UnableToReadTransformMeta" ), e );
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

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    if ( Utils.isEmpty( timeout ) ) {
      error_message = BaseMessages.getString( PKG, "DelayMeta.CheckResult.TimeOutMissing" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
    } else {
      error_message = BaseMessages.getString( PKG, "DelayMeta.CheckResult.TimeOutOk" );
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, error_message, transformMeta );
    }
    remarks.add( cr );

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.NotReceivingFields" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.TransformRecevingData2" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "DelayMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
    }
    remarks.add( cr );
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new Delay( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new DelayData();
  }
}
