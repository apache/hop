/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.pgpencryptstream;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.actions.pgpencryptfiles.GPG;

/**
 * Encrypt a stream with GPG *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class PGPEncryptStream extends BaseTransform<PGPEncryptStreamMeta, PGPEncryptStreamData> implements ITransform<PGPEncryptStreamMeta, PGPEncryptStreamData> {
  private static final Class<?> PKG = PGPEncryptStreamMeta.class; // For Translator

  public PGPEncryptStream( TransformMeta transformMeta,PGPEncryptStreamMeta meta, PGPEncryptStreamData data, int copyNr,
                           PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    try {
      if ( first ) {
        first = false;
        // get the RowMeta
        data.previousRowMeta = getInputRowMeta().clone();
        data.NrPrevFields = data.previousRowMeta.size();
        data.outputRowMeta = data.previousRowMeta;
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

        // Check is stream data field is provided
        if ( Utils.isEmpty( meta.getStreamField() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "PGPEncryptStream.Error.DataStreamFieldMissing" ) );
        }

        if ( meta.isKeynameInField() ) {
          // keyname will be extracted from a field
          String keyField = meta.getKeynameFieldName();
          if ( Utils.isEmpty( keyField ) ) {
            throw new HopException( BaseMessages.getString( PKG, "PGPEncryptStream.Error.KeyNameFieldMissing" ) );
          }
          data.indexOfKeyName = data.previousRowMeta.indexOfValue( keyField );
          if ( data.indexOfKeyName < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString(
              PKG, "PGPEncryptStream.Exception.CouldnotFindField", meta.getStreamField() ) );
          }
        } else {
          // Check is keyname is provided
          data.keyName = resolve( meta.getKeyName() );

          if ( Utils.isEmpty( data.keyName ) ) {
            throw new HopException( BaseMessages.getString( PKG, "PGPEncryptStream.Error.KeyNameMissing" ) );
          }
        }

        // cache the position of the field
        if ( data.indexOfField < 0 ) {
          data.indexOfField = data.previousRowMeta.indexOfValue( meta.getStreamField() );
          if ( data.indexOfField < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString(
              PKG, "PGPEncryptStream.Exception.CouldnotFindField", meta.getStreamField() ) );
          }
        }
      } // End If first

      // allocate output row
      Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      for ( int i = 0; i < data.NrPrevFields; i++ ) {
        outputRow[ i ] = r[ i ];
      }

      // get keyname if needed
      if ( meta.isKeynameInField() ) {
        // get keyname
        data.keyName = data.previousRowMeta.getString( r, data.indexOfKeyName );
        if ( Utils.isEmpty( data.keyName ) ) {
          throw new HopException( BaseMessages.getString( PKG, "PGPEncryptStream.Error.KeyNameMissing" ) );
        }
      }

      // get stream, data to encrypt
      String dataToEncrypt = data.previousRowMeta.getString( r, data.indexOfField );

      if ( Utils.isEmpty( dataToEncrypt ) ) {
        // no data..we can not continue with this row
        throw new HopException( BaseMessages.getString( PKG, "PGPEncryptStream.Error.DataToEncryptEmpty" ) );
      }

      // let's encrypt data
      String encryptedData = data.gpg.encrypt( dataToEncrypt, data.keyName );

      // Add encrypted data to input stream
      outputRow[ data.NrPrevFields ] = encryptedData;

      // add new values to the row.
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "PGPEncryptStream.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "PGPEncryptStream.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "PGPEncryptStreamO01" );
      }
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "PGPEncryptStream.Error.ResultFieldMissing" ) );
        return false;
      }

      try {
        // initiate a new GPG encryptor
        data.gpg = new GPG( resolve( meta.getGPGLocation() ), log );
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "PGPEncryptStream.Init.Error" ), e );
        return false;
      }

      return true;
    }
    return false;
  }

}
