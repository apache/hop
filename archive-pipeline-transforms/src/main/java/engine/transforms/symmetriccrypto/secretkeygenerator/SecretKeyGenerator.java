/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.symmetriccrypto.secretkeygenerator;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.CryptoException;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.SymmetricCrypto;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.SymmetricCryptoMeta;

import java.util.List;

/**
 * Generate secret key. for symmetric algorithms
 *
 * @author Samatar
 * @since 01-4-2011
 */
public class SecretKeyGenerator extends BaseTransform implements ITransform {
  private static final Class<?> PKG = SecretKeyGeneratorMeta.class; // for i18n purposes, needed by Translator!!

  private SecretKeyGeneratorMeta meta;

  private SecretKeyGeneratorData data;

  public SecretKeyGenerator( TransformMeta transformMeta, ITransformData data, int copyNr,
                             PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */

  private Object[] buildEmptyRow() {
    Object[] rowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

    return rowData;
  }

  public boolean processRow() throws HopException {

    Object[] row;
    Object[] rowIn = null;

    if ( data.readsRows ) {
      rowIn = getRow();
      if ( rowIn == null ) {
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;
        data.prevNrField = getInputRowMeta().size();
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      }

    } else {

      if ( first ) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
      }
    }
    for ( int i = 0; i < data.nr && !isStopped(); i++ ) {

      for ( int j = 0; j < data.secretKeyCount[ i ] && !isStopped(); j++ ) {

        // Create a new row
        row = buildEmptyRow();
        incrementLinesRead();

        int index = 0;

        try {
          // Return secret key
          if ( meta.isOutputKeyInBinary() ) {
            row[ index++ ] = data.crypto[ i ].generateKey( data.secretKeyLen[ i ] );
          } else {
            row[ index++ ] = data.crypto[ i ].generateKeyAsHex( data.secretKeyLen[ i ] );
          }

        } catch ( CryptoException k ) {
          throw new HopException( BaseMessages.getString( PKG, "SecretKeyGenerator.KeyGenerationError", i ), k );
        }

        if ( data.addAlgorithmOutput ) {
          // add algorithm
          row[ index++ ] = meta.getAlgorithm()[ i ];
        }

        if ( data.addSecretKeyLengthOutput ) {
          // add secret key len
          row[ index++ ] = new Long( data.secretKeyLen[ i ] );
        }

        if ( data.readsRows ) {
          // build output row
          row = RowDataUtil.addRowData( rowIn, data.prevNrField, row );
        }

        if ( isRowLevel() ) {
          logRowlevel( BaseMessages.getString( PKG, "SecretKeyGenerator.Log.ValueReturned", data.outputRowMeta
            .getString( row ) ) );
        }

        putRow( data.outputRowMeta, row );
      }
    }

    setOutputDone();
    return false;
  }

  public boolean init() {
    meta = (SecretKeyGeneratorMeta) smi;
    data = (SecretKeyGeneratorData) sdi;

    if ( super.init() ) {
      // Add init code here.

      if ( Utils.isEmpty( meta.getAlgorithm() ) ) {
        logError( BaseMessages.getString( PKG, "SecretKeyGenerator.Log.NoFieldSpecified" ) );
        return false;
      }

      if ( Utils.isEmpty( meta.getSecretKeyFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "SecretKeyGenerator.Log.secretKeyFieldMissing" ) );
        return false;
      }

      data.nr = meta.getAlgorithm().length;
      data.algorithm = new int[ data.nr ];
      data.scheme = new String[ data.nr ];
      data.secretKeyLen = new int[ data.nr ];
      data.secretKeyCount = new int[ data.nr ];

      for ( int i = 0; i < data.nr; i++ ) {
        data.algorithm[ i ] = SymmetricCryptoMeta.getAlgorithmTypeFromCode( meta.getAlgorithm()[ i ] );
        String len = environmentSubstitute( meta.getSecretKeyLength()[ i ] );
        data.secretKeyLen[ i ] = Const.toInt( len, -1 );
        if ( data.secretKeyLen[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "SecretKeyGenerator.Log.WrongLength", len, String.valueOf( i ) ) );
          return false;
        }
        String size = environmentSubstitute( meta.getSecretKeyCount()[ i ] );
        data.secretKeyCount[ i ] = Const.toInt( size, -1 );
        if ( data.secretKeyCount[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "SecretKeyGenerator.Log.WrongSize", size, String.valueOf( i ) ) );
          return false;
        }
        data.scheme[ i ] = environmentSubstitute( meta.getScheme()[ i ] );
      }

      data.readsRows = getTransformMeta().getRemoteInputTransforms().size() > 0;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms( getTransformMeta() );
      if ( previous != null && previous.size() > 0 ) {
        data.readsRows = true;
      }

      data.addAlgorithmOutput = !Utils.isEmpty( meta.getAlgorithmFieldName() );
      data.addSecretKeyLengthOutput = !Utils.isEmpty( meta.getSecretKeyLengthFieldName() );

      data.crypto = new SymmetricCrypto[ data.nr ];
      for ( int i = 0; i < data.nr; i++ ) {
        try {
          // Define a new crypto meta instance
          SymmetricCryptoMeta cryptoPipelineMeta = new SymmetricCryptoMeta( meta.getAlgorithm()[ i ] );

          // Initialize a new crypto object
          data.crypto[ i ] = new SymmetricCrypto( cryptoPipelineMeta, data.scheme[ i ] );

        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, "SecretKey.Init.Error" ), e );
          return false;
        }
      }

      return true;
    }
    return false;
  }

  public void.dispose() {
    super.dispose();
    if ( data.crypto != null ) {
      int nr = data.crypto.length;
      for ( int i = 0; i < nr; i++ ) {
        data.crypto[ i ].close();
      }
    }
  }

}
