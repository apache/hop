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

package org.apache.hop.pipeline.transforms.symmetriccrypto.symmetriccrypto;

import org.apache.commons.codec.binary.Hex;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.SymmetricCrypto;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.SymmetricCryptoMeta;

/**
 * Symmetric algorithm Executes a SymmetricCrypto on the values in the input stream. Selected calculated values can
 * then be put on the output stream.
 *
 * @author Samatar
 * @since 5-apr-2003
 */
public class SymmetricCrypto extends BaseTransform implements ITransform {
  private static Class<?> PKG = SymmetricCryptoPipelineMeta.class; // for i18n purposes, needed by Translator!!

  private SymmetricCryptoMeta meta;
  private SymmetricCryptoData data;

  public SymmetricCrypto( TransformMeta transformMeta, ITransformData iTransformData, int copyNr,
                          PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow( TransformMetaInterface smi, ITransformData sdi ) throws HopException {
    meta = (SymmetricCryptoMeta) smi;
    data = (SymmetricCryptoData) sdi;

    Object[] r = getRow(); // Get row from input rowset & set row busy!

    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }
    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      // Let's check that Result Field is given
      if ( Utils.isEmpty( meta.getResultfieldname() ) ) {
        // Result field is missing !
        throw new HopTransformException( BaseMessages.getString(
          PKG, "SymmetricCrypto.Exception.ErrorResultFieldMissing" ) );
      }

      // Check if The message field is given
      if ( Utils.isEmpty( meta.getMessageFied() ) ) {
        // Message Field is missing !
        throw new HopTransformException( BaseMessages.getString(
          PKG, "SymmetricCrypto.Exception.MissingMessageField" ) );
      }
      // Try to get Field index
      data.indexOfMessage = getInputRowMeta().indexOfValue( meta.getMessageFied() );

      // Let's check the Field
      if ( data.indexOfMessage < 0 ) {
        // The field is unreachable !
        throw new HopTransformException( BaseMessages.getString(
          PKG, "SymmetricCrypto.Exception.CouldnotFindField", meta.getMessageFied() ) );
      }

      if ( !meta.isSecretKeyInField() ) {
        String realSecretKey =
          Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( meta.getSecretKey() ) );
        if ( Utils.isEmpty( realSecretKey ) ) {
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SymmetricCrypto.Exception.SecretKeyMissing" ) );
        }
        // We have a static secret key
        // Set secrete key
        setSecretKey( realSecretKey );

      } else {
        // dynamic secret key
        if ( Utils.isEmpty( meta.getSecretKeyField() ) ) {
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SymmetricCrypto.Exception.SecretKeyFieldMissing" ) );
        }
        // Try to get secret key field index
        data.indexOfSecretkeyField = getInputRowMeta().indexOfValue( meta.getSecretKeyField() );

        // Let's check the Field
        if ( data.indexOfSecretkeyField < 0 ) {
          // The field is unreachable !
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SymmetricCrypto.Exception.CouldnotFindField", meta.getSecretKeyField() ) );
        }
      }

    }

    try {

      // handle dynamic secret key
      Object realSecretKey;
      if ( meta.isSecretKeyInField() ) {
        if ( meta.isReadKeyAsBinary() ) {
          realSecretKey = getInputRowMeta().getBinary( r, data.indexOfSecretkeyField );
          if ( realSecretKey == null ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "SymmetricCrypto.Exception.SecretKeyMissing" ) );
          }
        } else {
          realSecretKey =
            Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( getInputRowMeta().getString(
              r, data.indexOfSecretkeyField ) ) );
          if ( Utils.isEmpty( (String) realSecretKey ) ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "SymmetricCrypto.Exception.SecretKeyMissing" ) );
          }
        }

        // Set secrete key
        setSecretKey( realSecretKey );
      }

      // Get the field value

      Object result = null;

      if ( meta.getOperationType() == SymmetricCryptoPipelineMeta.OPERATION_TYPE_ENCRYPT ) {

        // encrypt plain text
        byte[] encrBytes = data.Crypt.encrDecryptData( getInputRowMeta().getBinary( r, data.indexOfMessage ) );

        // return encrypted value
        if ( meta.isOutputResultAsBinary() ) {
          result = encrBytes;
        } else {
          result = new String( Hex.encodeHex( ( encrBytes ) ) );
        }
      } else {
        // Get encrypted value
        String s = getInputRowMeta().getString( r, data.indexOfMessage );

        byte[] dataBytes = Hex.decodeHex( s.toCharArray() );

        // encrypt or decrypt message and return result
        byte[] encrBytes = data.Crypt.encrDecryptData( dataBytes );

        // we have decrypted value
        if ( meta.isOutputResultAsBinary() ) {
          result = encrBytes;
        } else {
          result = new String( encrBytes );
        }
      }

      Object[] outputRowData = RowDataUtil.addValueData( r, getInputRowMeta().size(), result );

      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);

    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage;
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "SymmetricCrypto.Log.ErrorInTransformRunning" ), e );
        logError( Const.getStackTracker( e ) );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, null, "EncDecr001" );
      }
    }

    return true;
  }

  public boolean init( TransformMetaInterface smi, ITransformData sdi ) {
    meta = (SymmetricCryptoPipelineMeta) smi;
    data = (SymmetricCryptoData) sdi;
    if ( super.init( smi, sdi ) ) {
      // Add init code here.

      try {
        // Define a new instance
        data.CryptMeta = new SymmetricCryptoMeta( meta.getAlgorithm() );
        // Initialize a new crypto pipeline object
        data.Crypt = new SymmetricCrypto( data.CryptMeta, environmentSubstitute( meta.getSchema() ) );

      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "SymmetricCrypto.ErrorInit." ), e );
        return false;
      }

      return true;
    }
    return false;
  }

  private void setSecretKey( Object key ) throws HopException {

    // Set secrete key
    if ( key instanceof byte[] ) {
      data.Crypt.setSecretKey( (byte[]) key );
    } else {
      data.Crypt.setSecretKey( (String) key );
    }

    if ( meta.getOperationType() == SymmetricCryptoPipelineMeta.OPERATION_TYPE_ENCRYPT ) {
      data.Crypt.setEncryptMode();
    } else {
      data.Crypt.setDecryptMode();
    }

  }

  public void dispose( TransformMetaInterface smi, ITransformData sdi ) {
    meta = (SymmetricCryptoPipelineMeta) smi;
    data = (SymmetricCryptoData) sdi;

    if ( data.Crypt != null ) {
      data.Crypt.close();
    }

    super.dispose( smi, sdi );
  }
}
