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

package org.apache.hop.core.database.util;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogTableCore;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.i18n.BaseMessages;

public class DatabaseLogExceptionFactory {

  public static final String HOP_GLOBAL_PROP_NAME = "HOP_FAIL_ON_LOGGING_ERROR";

  private static final ILogExceptionBehaviour throwable = new ThrowableBehaviour();
  private static final ILogExceptionBehaviour suppressable = new SuppressBehaviour();
  private static final ILogExceptionBehaviour suppressableWithShortMessage = new SuppressableWithShortMessage();

  /**
   * <p>
   * Returns throw exception strategy depends on defined behavior. Default is suppress exception.
   * </p>
   * <p/>
   * <p>
   * This behavior can be overridden with 'hop.properties' key-value using 'Edit hop.properties file' in HopGui or
   * other.
   * </p>
   * <p/>
   * <p>
   * Following this strategy - <code>System.getProperty(String key)</code> call will be used to check if key-value pair
   * is defined. If not found default behavior will be used. If not found and value is TRUE/Y - throwable behavior will
   * be used.
   * </p>
   *
   * @param table logging table that participated in exception. Must be instance of {@link ILogTableCore}, otherwise
   *              default suppress exception behavior will be used.
   * @return
   * @see {@link org.apache.hop.core.Const#HOP_VARIABLES_FILE}
   */
  public static ILogExceptionBehaviour getExceptionStrategy( ILogTableCore table ) {
    return getExceptionStrategy( table, null );
  }

  /**
   * <p>
   * Returns throw exception strategy depends on defined behavior. Default is suppress exception.
   * </p>
   * <p/>
   * <p>
   * This behavior can be overridden with 'hop.properties' key-value using 'Edit hop.properties file' in HopGui or
   * other.
   * </p>
   * <p/>
   * <p>
   * Following this strategy - <code>System.getProperty(String key)</code> call will be used to check if key-value pair
   * is defined. If key-value pair is not defined or value is set to FALSE/N, exception will be checked and suppressable strategy
   * with short message can be chosen, otherwise throwable behavior will be used
   * </p>
   *
   * @param table logging table that participated in exception. Must be instance of {@link ILogTableCore}, otherwise
   *              default suppress exception behavior will be used.
   * @param e     if key-value pair is not defined or value is set to FALSE/N, e will be checked and suppressable strategy
   *              with short message can be chosen.
   * @return
   * @see {@link org.apache.hop.core.Const#HOP_VARIABLES_FILE}
   */
  public static ILogExceptionBehaviour getExceptionStrategy( ILogTableCore table, Exception e ) {
    IDatabase iDatabase = extractDatabase( table );
    ILogExceptionBehaviour suppressableResult = suppressable;

    if ( iDatabase != null && !iDatabase.fullExceptionLog( e ) ) {
      suppressableResult = suppressableWithShortMessage;
    }

    String val = System.getProperty( HOP_GLOBAL_PROP_NAME );
    // with a small penalty for backward compatibility
    if ( val == null ) {
      // same as before
      return suppressableResult;
    }

    return ValueMetaBase.convertStringToBoolean( val ) ? throwable : suppressableResult;
  }

  private static IDatabase extractDatabase( ILogTableCore table ) {
    IDatabase result = null;
    if ( table != null && table.getDatabaseMeta() != null ) {
      return table.getDatabaseMeta().getIDatabase();
    }
    return result;
  }

  /**
   * Throw exception back to caller, this will be logged somewhere else.
   */
  private static class ThrowableBehaviour implements ILogExceptionBehaviour {

    @Override
    public void registerException( ILogChannel log, Class<?> packageClass, String key, String... parameters )
      throws HopDatabaseException {
      throw new HopDatabaseException( BaseMessages.getString( packageClass, key, parameters ) );
    }

    @Override public void registerException( ILogChannel log, Exception e, Class<?> packageClass, String key,
                                             String... parameters ) throws HopDatabaseException {
      throw new HopDatabaseException( BaseMessages.getString( packageClass, key, parameters ), e );
    }
  }

  /**
   * Suppress exception, but still add a log record about it
   */
  private static class SuppressBehaviour implements ILogExceptionBehaviour {

    @Override
    public void registerException( ILogChannel log, Class<?> packageClass, String key, String... parameters ) {
      log.logError( BaseMessages.getString( packageClass, key, parameters ) );
    }

    @Override public void registerException( ILogChannel log, Exception e, Class<?> packageClass, String key,
                                             String... parameters ) throws HopDatabaseException {
      log.logError( BaseMessages.getString( packageClass, key, parameters ), e );
    }
  }

  /**
   * Suppress exception, but still add a log record about it without stacktrace
   */
  private static class SuppressableWithShortMessage extends SuppressBehaviour {

    @Override public void registerException( ILogChannel log, Exception e, Class<?> packageClass, String key,
                                             String... parameters ) throws HopDatabaseException {
      registerException( log, packageClass, key, parameters );
      log.logError( e.getMessage() );
    }
  }
}
