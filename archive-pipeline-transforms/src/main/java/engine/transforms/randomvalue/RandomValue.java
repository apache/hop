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

package org.apache.hop.pipeline.transforms.randomvalue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.UUID4Util;
import org.apache.hop.core.util.UUIDUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Get random value.
 *
 * @author Matt, Samatar
 * @since 8-8-2008
 */
public class RandomValue extends BaseTransform implements TransformInterface {

  private static Class<?> PKG = RandomValueMeta.class; // for i18n purposes, needed by Translator!!

  private RandomValueMeta meta;

  private RandomValueData data;

  public RandomValue( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  private Object[] getRandomValue( RowMetaInterface inputRowMeta, Object[] inputRowData ) {
    Object[] row = new Object[ data.outputRowMeta.size() ];
    for ( int i = 0; i < inputRowMeta.size(); i++ ) {
      row[ i ] = inputRowData[ i ]; // no data is changed, clone is not
      // needed here.
    }

    for ( int i = 0, index = inputRowMeta.size(); i < meta.getFieldName().length; i++, index++ ) {
      switch ( meta.getFieldType()[ i ] ) {
        case RandomValueMeta.TYPE_RANDOM_NUMBER:
          row[ index ] = data.randomgen.nextDouble();
          break;
        case RandomValueMeta.TYPE_RANDOM_INTEGER:
          row[ index ] = new Long( data.randomgen.nextInt() ); // nextInt() already returns all 2^32 numbers.
          break;
        case RandomValueMeta.TYPE_RANDOM_STRING:
          row[ index ] = Long.toString( Math.abs( data.randomgen.nextLong() ), 32 );
          break;
        case RandomValueMeta.TYPE_RANDOM_UUID:
          row[ index ] = UUIDUtil.getUUIDAsString();
          break;
        case RandomValueMeta.TYPE_RANDOM_UUID4:
          row[ index ] = data.u4.getUUID4AsString();
          break;
        case RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5:
          try {
            row[ index ] = generateRandomMACHash( RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5 );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, "RandomValue.Log.ErrorGettingRandomHMACMD5", e.getMessage() ) );
            setErrors( 1 );
            stopAll();
          }
          break;
        case RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1:
          try {
            row[ index ] = generateRandomMACHash( RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1 );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, "RandomValue.Log.ErrorGettingRandomHMACSHA1", e.getMessage() ) );
            setErrors( 1 );
            stopAll();
          }
          break;
        default:
          break;
      }
    }

    return row;
  }

  private String generateRandomMACHash( int algorithm ) throws Exception {
    // Generates a secret key
    SecretKey sk = null;
    switch ( algorithm ) {
      case RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5:
        sk = data.keyGenHmacMD5.generateKey();
        break;
      case RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1:
        sk = data.keyGenHmacSHA1.generateKey();
        break;
      default:
        break;
    }

    if ( sk == null ) {
      throw new HopException( BaseMessages.getString( PKG, "RandomValue.Log.SecretKeyNull" ) );
    }

    // Create a MAC object using HMAC and initialize with key
    Mac mac = Mac.getInstance( sk.getAlgorithm() );
    mac.init( sk );
    // digest
    byte[] hashCode = mac.doFinal();
    StringBuilder encoded = new StringBuilder();
    for ( int i = 0; i < hashCode.length; i++ ) {
      String _b = Integer.toHexString( hashCode[ i ] );
      if ( _b.length() == 1 ) {
        _b = "0" + _b;
      }
      encoded.append( _b.substring( _b.length() - 2 ) );
    }

    return encoded.toString();

  }

  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {
    Object[] row;
    if ( data.readsRows ) {
      row = getRow();
      if ( row == null ) {
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
      }
    } else {
      row = new Object[] {}; // empty row
      incrementLinesRead();

      if ( first ) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
      }
    }

    RowMetaInterface imeta = getInputRowMeta();
    if ( imeta == null ) {
      imeta = new RowMeta();
      this.setInputRowMeta( imeta );
    }

    row = getRandomValue( imeta, row );

    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "RandomValue.Log.ValueReturned", data.outputRowMeta
        .getString( row ) ) );
    }

    putRow( data.outputRowMeta, row );

    if ( !data.readsRows ) { // Just one row and then stop!

      setOutputDone();
      return false;
    }

    return true;
  }

  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (RandomValueMeta) smi;
    data = (RandomValueData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.readsRows = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
      if ( previous != null && previous.size() > 0 ) {
        data.readsRows = true;
      }
      boolean genHmacMD5 = false;
      boolean genHmacSHA1 = false;
      boolean uuid4 = false;

      for ( int i = 0; i < meta.getFieldName().length; i++ ) {
        switch ( meta.getFieldType()[ i ] ) {
          case RandomValueMeta.TYPE_RANDOM_MAC_HMACMD5:
            genHmacMD5 = true;
            break;
          case RandomValueMeta.TYPE_RANDOM_MAC_HMACSHA1:
            genHmacSHA1 = true;
            break;
          case RandomValueMeta.TYPE_RANDOM_UUID4:
            uuid4 = true;
            break;
          default:
            break;
        }
      }
      if ( genHmacMD5 ) {
        try {
          data.keyGenHmacMD5 = KeyGenerator.getInstance( "HmacMD5" );
        } catch ( NoSuchAlgorithmException s ) {
          logError( BaseMessages.getString( PKG, "RandomValue.Log.HmacMD5AlgorithmException", s.getMessage() ) );
          return false;
        }
      }
      if ( genHmacSHA1 ) {
        try {
          data.keyGenHmacSHA1 = KeyGenerator.getInstance( "HmacSHA1" );
        } catch ( NoSuchAlgorithmException s ) {
          logError( BaseMessages.getString( PKG, "RandomValue.Log.HmacSHA1AlgorithmException", s.getMessage() ) );
          return false;
        }
      }
      if ( uuid4 ) {
        data.u4 = new UUID4Util();
      }
      return true;
    }
    return false;
  }

  public void dispose( TransformMetaInterface smi, TransformDataInterface sdi ) {
    super.dispose( smi, sdi );
  }

}
