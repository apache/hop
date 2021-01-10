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

package org.apache.hop.pipeline.transforms.salesforce;

import com.google.common.primitives.Ints;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Calendar;
import java.util.TimeZone;


public abstract class SalesforceTransform<Meta extends SalesforceTransformMeta, Data extends SalesforceTransformData>
    extends BaseTransform<Meta, Data>
    implements ITransform<Meta, Data> {

  public static Class<?> PKG = SalesforceTransform.class; // For Translator

  public SalesforceTransform( TransformMeta transformMeta, Meta meta, Data data, int copyNr, PipelineMeta pipelineMeta,
      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean init() {
    if ( !super.init( ) ) {
      return false;
    }

    String realUrl = resolve( meta.getTargetUrl() );
    String realUsername = resolve( meta.getUsername() );
    String realPassword = resolve( meta.getPassword() );
    String realModule = resolve( meta.getModule() );

    if ( Utils.isEmpty( realUrl ) ) {
      log.logError( BaseMessages.getString( PKG, "SalesforceConnection.TargetURLMissing.Error" ) );
      return false;
    }
    if ( Utils.isEmpty( realUsername ) ) {
      log.logError( BaseMessages.getString( PKG, "SalesforceConnection.UsernameMissing.Error" ) );
      return false;
    }
    if ( Utils.isEmpty( realPassword ) ) {
      log.logError( BaseMessages.getString( PKG, "SalesforceConnection.PasswordMissing.Error" ) );
      return false;
    }
    if ( Utils.isEmpty( realModule ) ) {
      log.logError( BaseMessages.getString( PKG, "SalesForceTransform.ModuleMissing.DialogMessage" ) );
      return false;
    }
    try {
      // The final transform should call data.connection.connect(), as other settings may set additional options
      data.connection = new SalesforceConnection( log, realUrl, realUsername, realPassword );
      data.connection.setModule( realModule );
      data.connection.setTimeOut( Const.toInt( resolve( meta.getTimeout() ), 0 ) );
      data.connection.setUsingCompression( meta.isCompression() );
    } catch ( HopException ke ) {
      logError( BaseMessages.getString( PKG, "SalesforceTransform.Log.ErrorOccurredDuringTransformInitialize" )
        + ke.getMessage() );
      return false;
    }
    return true;
  }

  public void dispose() {
    if ( data.connection != null ) {
      try {
        data.connection.close();
      } catch ( HopException ignored ) {
        /* Ignore */
      }
      data.connection = null;
    }
    super.dispose();
  }

  /**
   * normalize object for future sent in Salesforce
   *
   * @param valueMeta value meta
   * @param value pentaho internal value object
   * @return object for sending in Salesforce
   * @throws HopValueException
   */
  public Object normalizeValue( IValueMeta valueMeta, Object value ) throws HopValueException {
    if ( valueMeta.isDate() ) {
      // Pass date field converted to UTC
      //
      Calendar cal = Calendar.getInstance( valueMeta.getDateFormatTimeZone() );
      cal.setTime( valueMeta.getDate( value ) );
      Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
      // Reset time-related fields
      utc.clear();
      utc.set( cal.get( Calendar.YEAR ), cal.get( Calendar.MONTH ), cal.get( Calendar.DATE ),
        cal.get( Calendar.HOUR_OF_DAY ), cal.get( Calendar.MINUTE ), cal.get( Calendar.SECOND ) );
      value = utc;
    } else if ( valueMeta.isStorageBinaryString() ) {
      value = valueMeta.convertToNormalStorageType( value );
    }

    if ( IValueMeta.TYPE_INTEGER == valueMeta.getType() ) {
      // Salesforce integer values can be only http://www.w3.org/2001/XMLSchema:int
      // see org.apache.hop.ui.pipeline.transforms.salesforceinput.SalesforceInputDialog#addFieldToTable
      // So we need convert a Hop integer (real java Long value) to real int.
      // It will be sent correct as http://www.w3.org/2001/XMLSchema:int

      // use checked cast for prevent losing data
      value = Ints.checkedCast( (Long) value );
    }
    return value;
  }
}
