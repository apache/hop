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

package org.apache.hop.core.database.util;

import org.apache.hop.compatibility.ValueString;
import org.apache.hop.core.database.DatabaseInterface;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogTableCoreInterface;
import org.apache.hop.i18n.BaseMessages;

public class DatabaseLogExceptionFactory {

  public static final String HOP_GLOBAL_PROP_NAME = "HOP_FAIL_ON_LOGGING_ERROR";

  private static final LogExceptionBehaviourInterface throwable = new ThrowableBehaviour();
  private static final LogExceptionBehaviourInterface suppressable = new SuppressBehaviour();
  private static final LogExceptionBehaviourInterface suppressableWithShortMessage = new SuppressableWithShortMessage();

  /**
   * <p>
   * Returns throw exception strategy depends on defined behavior. Default is suppress exception.
   * </p>
   * <p/>
   * <p>
   * This behavior can be overridden with 'kettle.properties' key-value using 'Edit Hop.properties file' in Spoon or
   * other.
   * </p>
   * <p/>
   * <p>
   * Following this strategy - <code>System.getProperty(String key)</code> call will be used to check if key-value pair
   * is defined. If not found default behavior will be used. If not found and value is TRUE/Y - throwable behavior will
   * be used.
   * </p>
   *
   * @param table
   *     logging table that participated in exception. Must be instance of {@link LogTableCoreInterface}, otherwise
   *     default suppress exception behavior will be used.
   * @return
   * @see {@link org.apache.hop.core.Const#HOP_VARIABLES_FILE}
   */
  public static LogExceptionBehaviourInterface getExceptionStrategy( LogTableCoreInterface table ) {
    return getExceptionStrategy( table, null );
  }

  /**
   * <p>
   * Returns throw exception strategy depends on defined behavior. Default is suppress exception.
   * </p>
   * <p/>
   * <p>
   * This behavior can be overridden with 'kettle.properties' key-value using 'Edit Hop.properties file' in Spoon or
   * other.
   * </p>
   * <p/>
   * <p>
   * Following this strategy - <code>System.getProperty(String key)</code> call will be used to check if key-value pair
   * is defined. If key-value pair is not defined or value is set to FALSE/N, exception will be checked and suppressable strategy
   * with short message can be chosen, otherwise throwable behavior will be used
   * </p>
   *
   * @param table
   *     logging table that participated in exception. Must be instance of {@link LogTableCoreInterface}, otherwise
   *     default suppress exception behavior will be used.
   * @param e
   *     if key-value pair is not defined or value is set to FALSE/N, e will be checked and suppressable strategy
   *     with short message can be chosen.
   * @return
   * @see {@link org.apache.hop.core.Const#HOP_VARIABLES_FILE}
   */
  public static LogExceptionBehaviourInterface getExceptionStrategy( LogTableCoreInterface table, Exception e ) {
    DatabaseInterface databaseInterface = extractDatabase( table );
    LogExceptionBehaviourInterface suppressableResult = suppressable;

    if ( databaseInterface != null && !databaseInterface.fullExceptionLog( e ) ) {
      suppressableResult = suppressableWithShortMessage;
    }

    String val = System.getProperty( HOP_GLOBAL_PROP_NAME );
    // with a small penalty for backward compatibility
    if ( val == null ) {
      // same as before
      return suppressableResult;
    }

    ValueString sVal = new ValueString( val );
    return sVal.getBoolean() ? throwable : suppressableResult;
  }

  private static DatabaseInterface extractDatabase( LogTableCoreInterface table ) {
    DatabaseInterface result = null;
    if ( table != null && table.getDatabaseMeta() != null ) {
      return table.getDatabaseMeta().getDatabaseInterface();
    }
    return result;
  }

  /**
   * Throw exception back to caller, this will be logged somewhere else.
   */
  private static class ThrowableBehaviour implements LogExceptionBehaviourInterface {

    @Override
    public void registerException( LogChannelInterface log, Class<?> packageClass, String key, String... parameters )
        throws HopDatabaseException {
      throw new HopDatabaseException( BaseMessages.getString( packageClass, key, parameters ) );
    }

    @Override public void registerException( LogChannelInterface log, Exception e, Class<?> packageClass, String key,
        String... parameters ) throws HopDatabaseException {
      throw new HopDatabaseException( BaseMessages.getString( packageClass, key, parameters ), e );
    }
  }

  /**
   * Suppress exception, but still add a log record about it
   */
  private static class SuppressBehaviour implements LogExceptionBehaviourInterface {

    @Override
    public void registerException( LogChannelInterface log, Class<?> packageClass, String key, String... parameters ) {
      log.logError( BaseMessages.getString( packageClass, key, parameters ) );
    }

    @Override public void registerException( LogChannelInterface log, Exception e, Class<?> packageClass, String key,
        String... parameters ) throws HopDatabaseException {
      log.logError( BaseMessages.getString( packageClass, key, parameters ), e );
    }
  }

  /**
   * Suppress exception, but still add a log record about it without stacktrace
   */
  private static class SuppressableWithShortMessage extends SuppressBehaviour {

    @Override public void registerException( LogChannelInterface log, Exception e, Class<?> packageClass, String key,
        String... parameters ) throws HopDatabaseException {
      registerException( log, packageClass, key, parameters );
      log.logError( e.getMessage() );
    }
  }
}
