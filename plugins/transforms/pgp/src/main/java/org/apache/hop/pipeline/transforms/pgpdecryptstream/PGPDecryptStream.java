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

package org.apache.hop.pipeline.transforms.pgpdecryptstream;

import org.apache.hop.core.encryption.Encr;
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


public class PGPDecryptStream extends BaseTransform<PGPDecryptStreamMeta, PGPDecryptStreamData> implements ITransform<PGPDecryptStreamMeta, PGPDecryptStreamData> {
  private static final Class<?> PKG = PGPDecryptStreamMeta.class; // For Translator

  public PGPDecryptStream( TransformMeta transformMeta,PGPDecryptStreamMeta meta, PGPDecryptStreamData data, int copyNr,
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
          throw new HopException( BaseMessages.getString( PKG, "PGPDecryptStream.Error.DataStreamFieldMissing" ) );
        }

        if ( meta.isPassphraseFromField() ) {
          // Passphrase from field
          String fieldname = meta.getPassphraseFieldName();
          if ( Utils.isEmpty( fieldname ) ) {
            throw new HopException( BaseMessages.getString(
              PKG, "PGPDecryptStream.Error.PassphraseFieldMissing" ) );
          }
          data.indexOfPassphraseField = data.previousRowMeta.indexOfValue( fieldname );
          if ( data.indexOfPassphraseField < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString(
              PKG, "PGPDecryptStream.Exception.CouldnotFindField", fieldname ) );
          }
        } else {
          // Check is passphrase is provided
          data.passPhrase =
            Encr.decryptPasswordOptionallyEncrypted( resolve( meta.getPassphrase() ) );
          if ( Utils.isEmpty( data.passPhrase ) ) {
            throw new HopException( BaseMessages.getString( PKG, "PGPDecryptStream.Error.PassphraseMissing" ) );
          }
        }

        // cache the position of the field
        if ( data.indexOfField < 0 ) {
          data.indexOfField = data.previousRowMeta.indexOfValue( meta.getStreamField() );
          if ( data.indexOfField < 0 ) {
            // The field is unreachable !
            throw new HopException( BaseMessages.getString(
              PKG, "PGPDecryptStream.Exception.CouldnotFindField", meta.getStreamField() ) );
          }
        }
      } // End If first

      // allocate output row
      Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      for ( int i = 0; i < data.NrPrevFields; i++ ) {
        outputRow[ i ] = r[ i ];
      }

      if ( meta.isPassphraseFromField() ) {
        data.passPhrase = data.previousRowMeta.getString( r, data.indexOfPassphraseField );
        if ( Utils.isEmpty( data.passPhrase ) ) {
          throw new HopException( BaseMessages.getString( PKG, "PGPDecryptStream.Error.PassphraseMissing" ) );
        }
      }
      // get stream, data to decrypt
      String encryptedData = data.previousRowMeta.getString( r, data.indexOfField );

      if ( Utils.isEmpty( encryptedData ) ) {
        // no data..we can not continue with this row
        throw new HopException( BaseMessages.getString( PKG, "PGPDecryptStream.Error.DataToDecryptEmpty" ) );
      }

      // let's decrypt data
      String decryptedData = data.gpg.decrypt( encryptedData, data.passPhrase );

      // Add encrypted data to input stream
      outputRow[ data.NrPrevFields ] = decryptedData;

      // add new values to the row.
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "PGPDecryptStream.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "PGPDecryptStream.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "PGPDecryptStreamO01" );
      }
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "PGPDecryptStream.Error.ResultFieldMissing" ) );
        return false;
      }

      try {
        // initiate a new GPG encryptor
        data.gpg = new GPG( resolve( meta.getGPGLocation() ), log );
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "PGPDecryptStream.Init.Error" ), e );
        return false;
      }

      return true;
    }
    return false;
  }

}
