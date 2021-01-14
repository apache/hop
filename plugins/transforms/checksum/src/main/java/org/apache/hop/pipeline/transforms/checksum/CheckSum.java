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

package org.apache.hop.pipeline.transforms.checksum;

import org.apache.commons.codec.binary.Hex;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

/**
 * Caculate a checksum for each row.
 *
 * @author Samatar Hassan
 * @since 30-06-2008
 */
public class CheckSum extends BaseTransform<CheckSumMeta, CheckSumData> implements ITransform<CheckSumMeta, CheckSumData> {

  private static final Class<?> PKG = CheckSumMeta.class; // For Translator

  public CheckSum( TransformMeta transformMeta, CheckSumMeta meta, CheckSumData data, int copyNr, PipelineMeta pipelineMeta,
                   Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      data.nrInfields = data.outputRowMeta.size();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      if ( meta.getFieldName() == null || meta.getFieldName().length > 0 ) {
        data.fieldnrs = new int[ meta.getFieldName().length ];

        for ( int i = 0; i < meta.getFieldName().length; i++ ) {
          data.fieldnrs[ i ] = getInputRowMeta().indexOfValue( meta.getFieldName()[ i ] );
          if ( data.fieldnrs[ i ] < 0 ) {
            logError( BaseMessages.getString( PKG, "CheckSum.Log.CanNotFindField", meta.getFieldName()[ i ] ) );
            throw new HopException( BaseMessages.getString( PKG, "CheckSum.Log.CanNotFindField", meta
              .getFieldName()[ i ] ) );
          }
        }
      } else {
        data.fieldnrs = new int[ r.length ];
        for ( int i = 0; i < r.length; i++ ) {
          data.fieldnrs[ i ] = i;
        }
      }
      data.fieldnr = data.fieldnrs.length;

      try {
        if ( meta.getCheckSumType().equals( CheckSumMeta.TYPE_MD5 )
          || meta.getCheckSumType().equals( CheckSumMeta.TYPE_SHA1 )
          || meta.getCheckSumType().equals( CheckSumMeta.TYPE_SHA256 )
          || meta.getCheckSumType().equals( CheckSumMeta.TYPE_SHA384 )
          || meta.getCheckSumType().equals( CheckSumMeta.TYPE_SHA512 )) {
          data.digest = MessageDigest.getInstance( meta.getCheckSumType() ); // Sensitive
        }
      } catch ( Exception e ) {
        throw new HopException( BaseMessages.getString( PKG, "CheckSum.Error.Digest" ), e );
      }

    } // end if first

    Object[] outputRowData = null;

    try {
      if ( meta.getCheckSumType().equals( CheckSumMeta.TYPE_ADLER32 )
        || meta.getCheckSumType().equals( CheckSumMeta.TYPE_CRC32 ) ) {
        // get checksum
        Long checksum = calculCheckSum( r );
        outputRowData = RowDataUtil.addValueData( r, data.nrInfields, checksum );
      } else {
        // get checksum

        byte[] o = createCheckSum( r );

        switch ( meta.getResultType() ) {
          case CheckSumMeta.RESULT_TYPE_BINARY:
            outputRowData = RowDataUtil.addValueData( r, data.nrInfields, o );
            break;
          case CheckSumMeta.RESULT_TYPE_HEXADECIMAL:
            String hex = new String( Hex.encodeHex( o ) );
            outputRowData = RowDataUtil.addValueData( r, data.nrInfields, hex );
            break;
          default:
            outputRowData = RowDataUtil.addValueData( r, data.nrInfields, getStringFromBytes( o ) );
            break;
        }
      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "CheckSum.Log.LineNumber", Long.toString( getLinesRead() ) ) );
        }
      }

      // add new values to the row.
      putRow( data.outputRowMeta, outputRowData ); 
    } catch ( Exception e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "CheckSum.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        // signal end to receiver(s)
        setOutputDone();
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "CheckSum001" );
      }
    }
    return true;
  }

  private byte[] createCheckSum( Object[] r ) throws Exception {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Loop through fields
    for ( int i = 0; i < data.fieldnr; i++ ) {
      if ( getInputRowMeta().getValueMeta( data.fieldnrs[ i ] ).isBinary() ) {
        baos.write( getInputRowMeta().getBinary( r, data.fieldnrs[ i ] ) );
      } else {
        baos.write( getInputRowMeta().getValueMeta( data.fieldnrs[ i ] ).getNativeDataType( r[ data.fieldnrs[ i ] ] )
          .toString().getBytes() );
      }
    }

    // Updates the digest using the specified array of bytes
    data.digest.update( baos.toByteArray() );

    // Completes the hash computation by performing final operations such as padding
    byte[] hash = data.digest.digest();
    // After digest has been called, the MessageDigest object is reset to its initialized state

    return hash;
  }

  private static String getStringFromBytes( byte[] bytes ) {
    StringBuilder sb = new StringBuilder();
    for ( int i = 0; i < bytes.length; i++ ) {
      byte b = bytes[ i ];
      sb.append( 0x00FF & b );
      if ( i + 1 < bytes.length ) {
        sb.append( "-" );
      }
    }
    return sb.toString();
  }

  private Long calculCheckSum( Object[] r ) throws Exception {
    Long retval;
    byte[] byteArray;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Loop through fields
    for ( int i = 0; i < data.fieldnr; i++ ) {
      if ( getInputRowMeta().getValueMeta( data.fieldnrs[ i ] ).isBinary() ) {
         baos.write( getInputRowMeta().getBinary( r, data.fieldnrs[ i ] ) );
      } else {
         baos.write( getInputRowMeta().getValueMeta( data.fieldnrs[ i ] ).getNativeDataType( r[ data.fieldnrs[ i ] ] )
            .toString().getBytes() );
      }
    }
    byteArray = baos.toByteArray();
    
    if ( meta.getCheckSumType().equals( CheckSumMeta.TYPE_CRC32 ) ) {
      CRC32 crc32 = new CRC32();
      crc32.update( byteArray );
      retval = new Long( crc32.getValue() );
    } else {
      Adler32 adler32 = new Adler32();
      adler32.update( byteArray );
      retval = new Long( adler32.getValue() );
    }

    return retval;
  }

  @Override
  public boolean init(){
    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "CheckSum.Error.ResultFieldMissing" ) );
        return false;
      }

      return true;
    }
    return false;
  }

}
