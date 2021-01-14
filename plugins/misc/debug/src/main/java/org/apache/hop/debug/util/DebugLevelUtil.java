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

package org.apache.hop.debug.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.debug.action.ActionDebugLevel;
import org.apache.hop.debug.transform.TransformDebugLevel;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DebugLevelUtil {

  public static void storeTransformDebugLevel( Map<String, String> debugGroupAttributesMap, String transformName, TransformDebugLevel debugLevel )
    throws HopValueException, UnsupportedEncodingException {
    debugGroupAttributesMap.put( transformName + " : " + Defaults.TRANSFORM_ATTR_LOGLEVEL, debugLevel.getLogLevel().getCode() );
    debugGroupAttributesMap.put( transformName + " : " + Defaults.TRANSFORM_ATTR_START_ROW, Integer.toString( debugLevel.getStartRow() ) );
    debugGroupAttributesMap.put( transformName + " : " + Defaults.TRANSFORM_ATTR_END_ROW, Integer.toString( debugLevel.getEndRow() ) );

    String conditionXmlString = Base64.getEncoder().encodeToString( debugLevel.getCondition().getXml().getBytes( "UTF-8" ) );
    debugGroupAttributesMap.put( transformName + " : " + Defaults.TRANSFORM_ATTR_CONDITION, conditionXmlString );
  }

  public static TransformDebugLevel getTransformDebugLevel( Map<String, String> debugGroupAttributesMap, String transformName ) throws UnsupportedEncodingException, HopXmlException {


    String logLevelCode = debugGroupAttributesMap.get( transformName + " : " + Defaults.TRANSFORM_ATTR_LOGLEVEL );
    String startRowString = debugGroupAttributesMap.get( transformName + " : " + Defaults.TRANSFORM_ATTR_START_ROW );
    String endRowString = debugGroupAttributesMap.get( transformName + " : " + Defaults.TRANSFORM_ATTR_END_ROW );
    String conditionString = debugGroupAttributesMap.get( transformName + " : " + Defaults.TRANSFORM_ATTR_CONDITION );

    if ( StringUtils.isEmpty( logLevelCode ) ) {
      // Nothing to load
      //
      return null;
    }

    TransformDebugLevel debugLevel = new TransformDebugLevel();
    debugLevel.setLogLevel( LogLevel.getLogLevelForCode( logLevelCode ) );
    debugLevel.setStartRow( Const.toInt( startRowString, -1 ) );
    debugLevel.setEndRow( Const.toInt( endRowString, -1 ) );

    if ( StringUtils.isNotEmpty( conditionString ) ) {
      String conditionXml = new String( Base64.getDecoder().decode( conditionString ), "UTF-8" );
      debugLevel.setCondition( new Condition( conditionXml ) );
    }
    return debugLevel;
  }


  public static void clearDebugLevel( Map<String, String> debugGroupAttributesMap, String transformName ) {
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.TRANSFORM_ATTR_LOGLEVEL );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.TRANSFORM_ATTR_START_ROW );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.TRANSFORM_ATTR_END_ROW );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.TRANSFORM_ATTR_CONDITION );

    debugGroupAttributesMap.remove( transformName + " : " + Defaults.ACTION_ATTR_LOGLEVEL );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.ACTION_ATTR_LOG_RESULT );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.ACTION_ATTR_LOG_VARIABLES );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_ROWS );
    debugGroupAttributesMap.remove( transformName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_FILES );
  }

  public static void storeActionDebugLevel( Map<String, String> debugGroupAttributesMap, String entryName, ActionDebugLevel debugLevel ) throws HopValueException, UnsupportedEncodingException {
    debugGroupAttributesMap.put( entryName + " : " + Defaults.ACTION_ATTR_LOGLEVEL, debugLevel.getLogLevel().getCode() );
    debugGroupAttributesMap.put( entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT, debugLevel.isLoggingResult() ? "Y" : "N" );
    debugGroupAttributesMap.put( entryName + " : " + Defaults.ACTION_ATTR_LOG_VARIABLES, debugLevel.isLoggingVariables() ? "Y" : "N" );
    debugGroupAttributesMap.put( entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_ROWS, debugLevel.isLoggingResultRows() ? "Y" : "N" );
    debugGroupAttributesMap.put( entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_FILES, debugLevel.isLoggingResultFiles() ? "Y" : "N" );
  }

  public static ActionDebugLevel getActionDebugLevel( Map<String, String> debugGroupAttributesMap, String entryName ) throws UnsupportedEncodingException, HopXmlException {

    String logLevelCode = debugGroupAttributesMap.get( entryName + " : " + Defaults.ACTION_ATTR_LOGLEVEL );
    boolean loggingResult = "Y".equalsIgnoreCase( debugGroupAttributesMap.get( entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT ) );
    boolean loggingVariables = "Y".equalsIgnoreCase( debugGroupAttributesMap.get( entryName + " : " + Defaults.ACTION_ATTR_LOG_VARIABLES ) );
    boolean loggingResultRows = "Y".equalsIgnoreCase( debugGroupAttributesMap.get( entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_ROWS ) );
    boolean loggingResultFiles = "Y".equalsIgnoreCase( debugGroupAttributesMap.get( entryName + " : " + Defaults.ACTION_ATTR_LOG_RESULT_FILES ) );

    if ( StringUtils.isEmpty( logLevelCode ) ) {
      // Nothing to load
      //
      return null;
    }

    ActionDebugLevel debugLevel = new ActionDebugLevel();
    debugLevel.setLogLevel( LogLevel.getLogLevelForCode( logLevelCode ) );
    debugLevel.setLoggingResult( loggingResult );
    debugLevel.setLoggingVariables( loggingVariables );
    debugLevel.setLoggingResultRows( loggingResultRows );
    debugLevel.setLoggingResultFiles( loggingResultFiles );

    return debugLevel;
  }

  public static String getDurationHMS( double seconds ) {
    int day = (int) TimeUnit.SECONDS.toDays( (long) seconds );
    long hours = TimeUnit.SECONDS.toHours( (long) seconds ) - ( day * 24 );
    long minute = TimeUnit.SECONDS.toMinutes( (long) seconds ) - ( TimeUnit.SECONDS.toHours( (long) seconds ) * 60 );
    long second = TimeUnit.SECONDS.toSeconds( (long) seconds ) - ( TimeUnit.SECONDS.toMinutes( (long) seconds ) * 60 );
    long ms = (long) ( ( seconds - ( (long) seconds ) ) * 1000 );

    StringBuilder hms = new StringBuilder();
    if ( day > 0 ) {
      hms.append( day + "d " );
    }
    if ( day > 0 || hours > 0 ) {
      hms.append( hours + "h " );
    }
    if ( day > 0 || hours > 0 || minute > 0 ) {
      hms.append( String.format( "%2d", minute ) + "' " );
    }
    hms.append( String.format( "%2d", second ) + "." );
    hms.append( String.format( "%03d", ms ) + "\"" );

    return hms.toString();
  }
}
