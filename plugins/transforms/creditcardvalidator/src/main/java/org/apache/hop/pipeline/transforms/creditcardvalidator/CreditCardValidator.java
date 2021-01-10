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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Check if a Credit Card is valid *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class CreditCardValidator extends BaseTransform<CreditCardValidatorMeta, CreditCardValidatorData> implements ITransform<CreditCardValidatorMeta, CreditCardValidatorData> {

  private static final Class<?> PKG = CreditCardValidatorMeta.class; // For Translator

  public CreditCardValidator( TransformMeta transformMeta, CreditCardValidatorMeta meta, CreditCardValidatorData data, int copyNr,
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

    boolean isValid = false;
    String cardType = null;
    String unValid = null;

    if ( first ) {
      first = false;

      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.NrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Check if field is provided
      if ( Utils.isEmpty( meta.getDynamicField() ) ) {
        logError( BaseMessages.getString( PKG, "CreditCardValidator.Error.CardFieldMissing" ) );
        throw new HopException( BaseMessages.getString( PKG, "CreditCardValidator.Error.CardFieldMissing" ) );
      }

      // cache the position of the field
      if ( data.indexOfField < 0 ) {
        data.indexOfField = getInputRowMeta().indexOfValue( meta.getDynamicField() );
        if ( data.indexOfField < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString(
            PKG, "CreditCardValidator.Exception.CouldnotFindField", meta.getDynamicField() ) );
        }
      }
      data.realResultFieldname = resolve( meta.getResultFieldName() );
      if ( Utils.isEmpty( data.realResultFieldname ) ) {
        throw new HopException( BaseMessages
          .getString( PKG, "CreditCardValidator.Exception.ResultFieldMissing" ) );
      }
      data.realCardTypeFieldname = resolve( meta.getCardType() );
      data.realNotValidMsgFieldname = resolve( meta.getNotValidMsg() );

    } // End If first

    Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    for ( int i = 0; i < data.NrPrevFields; i++ ) {
      outputRow[ i ] = r[ i ];
    }
    try {
      // get field
      String fieldvalue = getInputRowMeta().getString( r, data.indexOfField );
      if ( meta.isOnlyDigits() ) {
        fieldvalue = Const.getDigitsOnly( fieldvalue );
      }

      ReturnIndicator rt = CreditCardVerifier.CheckCC( fieldvalue );

      // Check if Card is Valid?
      isValid = rt.CardValid;
      // include Card Type?
      if ( !Utils.isEmpty( data.realCardTypeFieldname ) ) {
        cardType = rt.CardType;
      }
      // include Not valid message?
      if ( !Utils.isEmpty( data.realNotValidMsgFieldname ) ) {
        unValid = rt.UnValidMsg;
      }

      // add card is Valid
      outputRow[ data.NrPrevFields ] = isValid;
      int rowIndex = data.NrPrevFields;
      rowIndex++;

      // add card type?
      if ( !Utils.isEmpty( data.realCardTypeFieldname ) ) {
        outputRow[ rowIndex++ ] = cardType;
      }
      // add not valid message?
      if ( !Utils.isEmpty( data.realNotValidMsgFieldname ) ) {
        outputRow[ rowIndex++ ] = unValid;
      }

      // add new values to the row.
      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "CreditCardValidator.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }

    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "CreditCardValidator.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "CreditCardValidator001" );
      }
    }

    return true;
  }

  public boolean init(){
    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "CreditCardValidator.Error.ResultFieldMissing" ) );
        return false;
      }
      return true;
    }
    return false;
  }
}
