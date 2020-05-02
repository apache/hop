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

package org.apache.hop.databases.mysql.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.util.DatabaseLogExceptionFactory;
import org.apache.hop.core.database.util.ILogExceptionBehaviour;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogTableCore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.databases.mysql.MySqlDatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.PacketTooBigException;

public class MySqlDatabaseLogExceptionFactoryTest {

  private ILogTableCore logTable;
  private static final String THROWABLE = "org.apache.hop.core.database.util.DatabaseLogExceptionFactory$ThrowableBehaviour";
  private static final String SUPPRESSABLE_WITH_SHORT_MESSAGE = "org.apache.hop.core.database.util.DatabaseLogExceptionFactory$SuppressableWithShortMessage";
  private static final String PROPERTY_VALUE_TRUE = "Y";

  @ClassRule
  public static RestoreHopEnvironment env = new RestoreHopEnvironment();
	
  @BeforeClass
  public static void setUpBeforeClass() throws HopException {		
		PluginRegistry.addPluginType(DatabasePluginType.getInstance());
		PluginRegistry.init();	
  }
  
  @Before public void setUp() {
    logTable = mock( ILogTableCore.class );
    System.clearProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME );
  }

  @After public void tearDown() {
    System.clearProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME );
  }

  /**
   * PDI-5153
   * Test that in case of PacketTooBigException exception there will be no stack trace in log
   */
  @Test
  public void testExceptionStrategyWithPacketTooBigException() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new MySqlDatabaseMeta());
    PacketTooBigException e = new PacketTooBigException();

    when( logTable.getDatabaseMeta() ).thenReturn( databaseMeta );

    ILogExceptionBehaviour
      exceptionStrategy =
      DatabaseLogExceptionFactory.getExceptionStrategy( logTable, new HopDatabaseException( e ) );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( SUPPRESSABLE_WITH_SHORT_MESSAGE, strategyName );
  }

  /**
   * PDI-5153
   * Test that in case of MysqlDataTruncation exception there will be no stack trace in log
   */
  @Test public void testExceptionStrategyWithMysqlDataTruncationException() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new MySqlDatabaseMeta());
    MysqlDataTruncation e = new MysqlDataTruncation();

    when( logTable.getDatabaseMeta() ).thenReturn( databaseMeta );


    ILogExceptionBehaviour
      exceptionStrategy =
      DatabaseLogExceptionFactory.getExceptionStrategy( logTable, new HopDatabaseException( e ) );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( SUPPRESSABLE_WITH_SHORT_MESSAGE, strategyName );
  }

  /**
   * Property value has priority
   */
  @Test public void testExceptionStrategyWithPacketTooBigExceptionPropSetY() {
    System.setProperty( DatabaseLogExceptionFactory.HOP_GLOBAL_PROP_NAME, PROPERTY_VALUE_TRUE );

    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new MySqlDatabaseMeta());
    PacketTooBigException e = new PacketTooBigException();

    when( logTable.getDatabaseMeta() ).thenReturn( databaseMeta );


    ILogExceptionBehaviour
      exceptionStrategy =
      DatabaseLogExceptionFactory.getExceptionStrategy( logTable, new HopDatabaseException( e ) );
    String strategyName = exceptionStrategy.getClass().getName();
    assertEquals( THROWABLE, strategyName );
  }
}
