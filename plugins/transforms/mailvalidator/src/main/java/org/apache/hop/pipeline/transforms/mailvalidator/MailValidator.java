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

package org.apache.hop.pipeline.transforms.mailvalidator;

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
 * Check if an email address is valid *
 *
 * @author Samatar
 * @since 03-Juin-2008
 */

public class MailValidator extends BaseTransform<MailValidatorMeta, MailValidatorData> implements ITransform<MailValidatorMeta, MailValidatorData> {
  private static final Class<?> PKG = MailValidator.class; // For Translator

  public MailValidator(TransformMeta transformMeta, MailValidatorMeta meta, MailValidatorData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

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
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // check result fieldname
      data.realResultFieldName = resolve( meta.getResultFieldName() );
      if ( Utils.isEmpty( data.realResultFieldName ) ) {
        throw new HopException( BaseMessages.getString( PKG, "MailValidator.Error.ResultFieldNameMissing" ) );
      }

      if ( meta.isResultAsString() ) {
        if ( Utils.isEmpty( meta.getEMailValideMsg() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "MailValidator.Error.EMailValidMsgMissing" ) );
        }

        if ( Utils.isEmpty( meta.getEMailNotValideMsg() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "MailValidator.Error.EMailNotValidMsgMissing" ) );
        }

        data.msgValidMail = resolve( meta.getEMailValideMsg() );
        data.msgNotValidMail = resolve( meta.getEMailNotValideMsg() );
      }

      // Check is email address field is provided
      if ( Utils.isEmpty( meta.getEmailField() ) ) {
        throw new HopException( BaseMessages.getString( PKG, "MailValidator.Error.FilenameFieldMissing" ) );
      }

      data.realResultErrorsFieldName = resolve( meta.getErrorsField() );

      // cache the position of the field
      if ( data.indexOfeMailField < 0 ) {
        data.indexOfeMailField = data.previousRowMeta.indexOfValue( meta.getEmailField() );
        if ( data.indexOfeMailField < 0 ) {
          // The field is unreachable !
          throw new HopException( BaseMessages.getString(
            PKG, "MailValidator.Exception.CouldnotFindField", meta.getEmailField() ) );
        }
      }

      // SMTP check?
      if ( meta.isSMTPCheck() ) {
        if ( meta.isdynamicDefaultSMTP() ) {
          if ( Utils.isEmpty( meta.getDefaultSMTP() ) ) {
            throw new HopException( BaseMessages.getString( PKG, "MailValidator.Error.DefaultSMTPFieldMissing" ) );
          }

          if ( data.indexOfdefaultSMTPField < 0 ) {
            data.indexOfdefaultSMTPField = data.previousRowMeta.indexOfValue( meta.getDefaultSMTP() );
            if ( data.indexOfdefaultSMTPField < 0 ) {
              // The field is unreachable !
              throw new HopException( BaseMessages.getString(
                PKG, "MailValidator.Exception.CouldnotFindField", meta.getDefaultSMTP() ) );
            }
          }
        }
        // get Timeout
        data.timeout = Const.toInt( resolve( meta.getTimeOut() ), 0 );

        // get email sender
        data.realemailSender = resolve( meta.geteMailSender() );

        // get default SMTP server
        data.realdefaultSMTPServer = resolve( meta.getDefaultSMTP() );
      }

    } // End If first

    boolean sendToErrorRow = false;
    String errorMessage = null;
    boolean mailvalid = false;
    String mailerror = null;

    Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    for ( int i = 0; i < data.NrPrevFields; i++ ) {
      outputRow[ i ] = r[ i ];
    }

    try {
      // get dynamic email address
      String emailaddress = data.previousRowMeta.getString( r, data.indexOfeMailField );

      if ( !Utils.isEmpty( emailaddress ) ) {
        if ( meta.isdynamicDefaultSMTP() ) {
          data.realdefaultSMTPServer = data.previousRowMeta.getString( r, data.indexOfdefaultSMTPField );
        }

        // Check if address is valid
        MailValidationResult result =
          MailValidation.isAddressValid(
            log, emailaddress, data.realemailSender, data.realdefaultSMTPServer, data.timeout, meta
              .isSMTPCheck() );
        // return result
        mailvalid = result.isValide();
        mailerror = result.getErrorMessage();

      } else {
        mailerror = BaseMessages.getString( PKG, "MailValidator.Error.MailEmpty" );
      }
      if ( meta.isResultAsString() ) {
        if ( mailvalid ) {
          outputRow[ data.NrPrevFields ] = data.msgValidMail;
        } else {
          outputRow[ data.NrPrevFields ] = data.msgNotValidMail;
        }
      } else {
        // add boolean result field
        outputRow[ data.NrPrevFields ] = mailvalid;
      }
      int rowIndex = data.NrPrevFields;
      rowIndex++;
      // add errors field
      if ( !Utils.isEmpty( data.realResultErrorsFieldName ) ) {
        outputRow[ rowIndex ] = mailerror;
      }

      putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "MailValidator.LineNumber", getLinesRead()
          + " : " + getInputRowMeta().getString( r ) ) );
      }
    } catch ( Exception e ) {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "MailValidator.ErrorInTransformRunning" ) + e.getMessage() );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;

      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), r, 1, errorMessage, meta.getResultFieldName(), "MailValidator001" );
      }
    }

    return true;
  }

  public boolean init() {

    if ( super.init() ) {
      if ( Utils.isEmpty( meta.getResultFieldName() ) ) {
        logError( BaseMessages.getString( PKG, "MailValidator.Error.ResultFieldMissing" ) );
        return false;
      }
      return true;
    }
    return false;
  }

  public void dispose() {

    super.dispose();
  }

}
