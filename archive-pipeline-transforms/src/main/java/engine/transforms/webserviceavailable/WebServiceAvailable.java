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

package org.apache.hop.pipeline.transforms.webserviceavailable;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * Check if a webservice is available *
 *
 * @author Samatar
 * @since 03-01-2010
 */

public class WebServiceAvailable extends BaseTransform implements ITransform {
  private static Class<?> PKG = WebServiceAvailableMeta.class; // for i18n purposes, needed by Translator!!

  private WebServiceAvailableMeta meta;
  private WebServiceAvailableData data;

  public WebServiceAvailable( TransformMeta transformMeta, ITransformData data, int copyNr,
                              PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (WebServiceAvailableMeta) smi;
    data = (WebServiceAvailableData) sdi;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      // Check is URL field is provided
      if ( Utils.isEmpty( meta.getURLField() ) ) {
        logError( BaseMessages.getString( PKG, "WebServiceAvailable.Error.FilenameFieldMissing" ) );
        throw new HopException( BaseMessages.getString( PKG, "WebServiceAvailable.Error.FilenameFieldMissing" ) );
      }

      // cache the position of the field
      data.indexOfURL = data.previousRowMeta.indexOfValue( meta.getURLField() );
      if ( data.indexOfURL < 0 ) {
        // The field is unreachable !
        logError( BaseMessages.getString( PKG, "WebServiceAvailable.Exception.CouldnotFindField" )
          + "[" + meta.getURLField() + "]" );
        throw new HopException( BaseMessages.getString(
          PKG, "WebServiceAvailable.Exception.CouldnotFindField", meta.getURLField() ) );
      }
    } // End If first

    try {

      // get url
      String url = data.previousRowMeta.getString( r, data.indexOfURL );

      if ( Utils.isEmpty( url ) ) {
        throw new HopException( BaseMessages.getString( PKG, "WebServiceAvailable.Error.URLEmpty" ) );
      }

      if ( isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "WebServiceAvailable.Log.CheckingURL", url ) );
      }

      boolean WebServiceAvailable = false;

      InputStream in = null;

      try {
        URLConnection conn = new URL( url ).openConnection();
        conn.setConnectTimeout( data.connectTimeOut );
        conn.setReadTimeout( data.readTimeOut );
        in = conn.getInputStream();
        // Web service is available
        WebServiceAvailable = true;
      } catch ( Exception e ) {
        if ( isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "WebServiceAvailable.Error.ServiceNotReached", url, e.toString() ) );
        }

      } finally {
        if ( in != null ) {
          try {
            in.close();
          } catch ( Exception e ) { /* Ignore */
          }
        }
      }

      // addwebservice available to the row
      putRow( data.outputRowMeta, RowDataUtil.addValueData( r, data.NrPrevFields, WebServiceAvailable ) ); // copy row
      // to output
      // rowset(s);

      if ( isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "FileExists.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "WebServiceAvailable.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "WebServiceAvailable001" );
      }
    }

    return true;
  }

  public boolean init() {
    meta = (WebServiceAvailableMeta) smi;
    data = (WebServiceAvailableData) sdi;

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "WebServiceAvailable.Error.ResultFieldMissing" ) );
        return false;
      }
      data.connectTimeOut = Const.toInt( environmentSubstitute( meta.getConnectTimeOut() ), 0 );
      data.readTimeOut = Const.toInt( environmentSubstitute( meta.getReadTimeOut() ), 0 );
      return true;
    }
    return false;
  }

  public void.dispose() {
    meta = (WebServiceAvailableMeta) smi;
    data = (WebServiceAvailableData) sdi;

    super.dispose();
  }
}
