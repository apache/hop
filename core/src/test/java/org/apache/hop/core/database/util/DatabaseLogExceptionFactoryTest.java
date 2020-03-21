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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.logging.LogTableCoreInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DatabaseLogExceptionFactoryTest {

  private LogTableCoreInterface logTable;
  private static final String SUPPRESSABLE = "org.apache.hop.core.database.util.DatabaseLogExceptionFactory$SuppressBehaviour";
  private static final String THROWABLE = "org.apache.hop.core.database.util.DatabaseLogExceptionFactory$ThrowableBehaviour";
  private static final String
    SUPPRESSABLE_WITH_SHORT_MESSAGE =
    "org.apache.hop.core.database.util.DatabaseLogExceptionFactory$SuppressableWithShortMessage";
  private static final String PROPERTY_VALUE_TRUE = "Y";

  @Before public void setUp() {
    logTable = mock( LogTableCoreInterface.class );
    System.clearProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME );
  }

  @After public void tearDown() {
    System.clearProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME );
  }

  @Test public void testGetExceptionStrategyWithoutException() {
    LogExceptionBehaviourInterface exceptionStrategy = DatabaseLogExceptionFactory.getExceptionStrategy( logTable );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( SUPPRESSABLE, strategyName );
  }

  @Test public void testGetExceptionStrategyWithoutExceptionPropSetY() {
    System.setProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME, PROPERTY_VALUE_TRUE );
    LogExceptionBehaviourInterface exceptionStrategy = DatabaseLogExceptionFactory.getExceptionStrategy( logTable );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( THROWABLE, strategyName );
  }

  @Test public void testExceptionStrategyWithException() {
    LogExceptionBehaviourInterface
      exceptionStrategy =
      DatabaseLogExceptionFactory.getExceptionStrategy( logTable, new Exception() );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( SUPPRESSABLE, strategyName );
  }

  @Test public void testGetExceptionStrategyWithExceptionPropSetY() {
    System.setProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME, PROPERTY_VALUE_TRUE );
    LogExceptionBehaviourInterface
      exceptionStrategy =
      DatabaseLogExceptionFactory.getExceptionStrategy( logTable, new Exception() );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( THROWABLE, strategyName );
  }

 

}
