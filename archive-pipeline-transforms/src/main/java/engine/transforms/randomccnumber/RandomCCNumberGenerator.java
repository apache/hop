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

package org.apache.hop.pipeline.transforms.randomccnumber;

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

/**
 * Generate random credit card number.
 *
 * @author Samatar
 * @since 01-4-2010
 */
public class RandomCCNumberGenerator extends BaseTransform implements ITransform {
  private static Class<?> PKG = RandomCCNumberGeneratorMeta.class; // for i18n purposes, needed by Translator!!

  private RandomCCNumberGeneratorMeta meta;

  private RandomCCNumberGeneratorData data;

  public RandomCCNumberGenerator( TransformMeta transformMeta, ITransformData data, int copyNr,
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

    if ( first ) {
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
    }
    for ( int i = 0; i < data.cardTypes.length && !isStopped(); i++ ) {

      // Return card numbers
      String[] cardNumber =
        RandomCreditCardNumberGenerator.GenerateCreditCardNumbers(
          data.cardTypes[ i ], data.cardLen[ i ], data.cardSize[ i ] );

      for ( int j = 0; j < cardNumber.length && !isStopped(); j++ ) {
        // Create a new row
        Object[] row = buildEmptyRow();
        incrementLinesRead();

        int index = 0;
        // add card number
        row[ index++ ] = cardNumber[ j ];

        if ( data.addCardTypeOutput ) {
          // add card type
          row[ index++ ] = meta.getFieldCCType()[ i ];
        }

        if ( data.addCardLengthOutput ) {
          // add card len
          row[ index++ ] = new Long( data.cardLen[ i ] );
        }
        if ( isRowLevel() ) {
          logRowlevel( BaseMessages.getString(
            PKG, "RandomCCNumberGenerator.Log.ValueReturned", data.outputRowMeta.getString( row ) ) );
        }

        putRow( data.outputRowMeta, row );
      }
    }

    setOutputDone();
    return false;
  }

  public boolean init() {
    meta = (RandomCCNumberGeneratorMeta) smi;
    data = (RandomCCNumberGeneratorData) sdi;

    if ( super.init() ) {
      // Add init code here.

      if ( meta.getFieldCCType() == null ) {
        logError( BaseMessages.getString( PKG, "RandomCCNumberGenerator.Log.NoFieldSpecified" ) );
        return false;
      }
      if ( meta.getFieldCCType().length == 0 ) {
        logError( BaseMessages.getString( PKG, "RandomCCNumberGenerator.Log.NoFieldSpecified" ) );
        return false;
      }

      if ( Utils.isEmpty( meta.getCardNumberFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "RandomCCNumberGenerator.Log.CardNumberFieldMissing" ) );
        return false;
      }

      data.cardTypes = new int[ meta.getFieldCCType().length ];
      data.cardLen = new int[ meta.getFieldCCType().length ];
      data.cardSize = new int[ meta.getFieldCCType().length ];

      for ( int i = 0; i < meta.getFieldCCType().length; i++ ) {
        data.cardTypes[ i ] = RandomCreditCardNumberGenerator.getCardType( meta.getFieldCCType()[ i ] );
        String len = environmentSubstitute( meta.getFieldCCLength()[ i ] );
        data.cardLen[ i ] = Const.toInt( len, -1 );
        if ( data.cardLen[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "RandomCCNumberGenerator.Log.WrongLength", len, String
            .valueOf( i ) ) );
          return false;
        }
        String size = environmentSubstitute( meta.getFieldCCSize()[ i ] );
        data.cardSize[ i ] = Const.toInt( size, -1 );
        if ( data.cardSize[ i ] < 0 ) {
          logError( BaseMessages
            .getString( PKG, "RandomCCNumberGenerator.Log.WrongSize", size, String.valueOf( i ) ) );
          return false;
        }
      }

      data.addCardTypeOutput = !Utils.isEmpty( meta.getCardTypeFieldName() );
      data.addCardLengthOutput = !Utils.isEmpty( meta.getCardLengthFieldName() );

      return true;
    }
    return false;
  }

  public void.dispose() {
    super.dispose();
  }

}
