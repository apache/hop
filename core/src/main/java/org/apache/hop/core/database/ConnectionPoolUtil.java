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

package org.apache.hop.core.database;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.database.util.DatabaseUtil;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.i18n.BaseMessages;

import javax.sql.DataSource;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolUtil {

  public static final String DEFAULT_AUTO_COMMIT = "defaultAutoCommit";
  public static final String DEFAULT_READ_ONLY = "defaultReadOnly";
  public static final String DEFAULT_TRANSACTION_ISOLATION = "defaultTransactionIsolation";
  public static final String DEFAULT_CATALOG = "defaultCatalog";
  public static final String INITIAL_SIZE = "initialSize";
  public static final String MAX_ACTIVE = "maxActive";
  public static final String MAX_IDLE = "maxIdle";
  public static final String MIN_IDLE = "minIdle";
  public static final String MAX_WAIT = "maxWait";
  public static final String VALIDATION_QUERY = "validationQuery";
  public static final String TEST_ON_BORROW = "testOnBorrow";
  public static final String TEST_ON_RETURN = "testOnReturn";
  public static final String TEST_WHILE_IDLE = "testWhileIdle";
  public static final String TIME_BETWEEN_EVICTION_RUNS_MILLIS = "timeBetweenEvictionRunsMillis";
  public static final String POOL_PREPARED_STATEMENTS = "poolPreparedStatements";
  public static final String MAX_OPEN_PREPARED_STATEMENTS = "maxOpenPreparedStatements";
  public static final String ACCESS_TO_UNDERLYING_CONNECTION_ALLOWED = "accessToUnderlyingConnectionAllowed";
  public static final String REMOVE_ABANDONED = "removeAbandoned";
  public static final String REMOVE_ABANDONED_TIMEOUT = "removeAbandonedTimeout";
  public static final String LOG_ABANDONED = "logAbandoned";
  private static Class<?> PKG = Database.class; // for i18n purposes, needed by Translator2!!

  private static ConcurrentMap<String, BasicDataSource> dataSources = new ConcurrentHashMap<String, BasicDataSource>();

  // PDI-12947
  private static final ReentrantLock lock = new ReentrantLock();

  public static final int defaultInitialNrOfConnections = 5;
  public static final int defaultMaximumNrOfConnections = 10;

  private static boolean isDataSourceRegistered( DatabaseMeta dbMeta, String partitionId )
    throws HopDatabaseException {
    try {
      String name = getDataSourceName( dbMeta, partitionId );
      return dataSources.containsKey( name );
    } catch ( Exception e ) {
      throw new HopDatabaseException( BaseMessages.getString( PKG,
          "Database.UnableToCheckIfConnectionPoolExists.Exception" ), e );
    }
  }

  public static Connection getConnection( LogChannelInterface log, DatabaseMeta dbMeta, String partitionId )
    throws Exception {
    return getConnection( log, dbMeta, partitionId, dbMeta.getInitialPoolSize(), dbMeta.getMaximumPoolSize() );
  }

  public static Connection getConnection( LogChannelInterface log, DatabaseMeta dbMeta, String partitionId,
      int initialSize, int maximumSize ) throws Exception {
    lock.lock();
    try {
      if ( !isDataSourceRegistered( dbMeta, partitionId ) ) {
        addPoolableDataSource( log, dbMeta, partitionId, initialSize, maximumSize );
      }
    } finally {
      lock.unlock();
    }
    BasicDataSource ds = dataSources.get( getDataSourceName( dbMeta, partitionId ) );
    return ds.getConnection();
  }

  // BACKLOG-674
  private static String getDataSourceName( DatabaseMeta dbMeta, String partitionId ) {
    return dbMeta.getName() + Const.NVL( dbMeta.getDatabaseName(), "" ) + Const.NVL( dbMeta.getHostname(), "" )
        + Const.NVL( dbMeta.getPort(), "" ) + Const.NVL( partitionId, "" );
  }

  /**
   * Replace Hop variables/parameters with its values
   *
   * @param properties
   * @param databaseMeta
   * @return new object of type Properties with resolved variables/parameters
   */
  private static Properties environmentSubstitute( Properties properties, DatabaseMeta databaseMeta ) {
    Iterator<Object> iterator = properties.keySet().iterator();
    while ( iterator.hasNext() ) {
      String key = (String) iterator.next();
      String value = properties.getProperty( key );
      properties.put( key, databaseMeta.environmentSubstitute( value ) );
    }
    return properties;
  }

  @VisibleForTesting
  static void configureDataSource( BasicDataSource ds, DatabaseMeta databaseMeta, String partitionId,
      int initialSize, int maximumSize ) throws HopDatabaseException {
    // substitute variables and populate pool properties; add credentials
    Properties connectionPoolProperties = new Properties( databaseMeta.getConnectionPoolingProperties() );
    connectionPoolProperties = environmentSubstitute( connectionPoolProperties, databaseMeta );
    setPoolProperties( ds, connectionPoolProperties, initialSize, maximumSize );
    setCredentials( ds, databaseMeta, partitionId );

    // add url/driver class
    String url = databaseMeta.environmentSubstitute( databaseMeta.getURL( partitionId ) );
    ds.setUrl( url );
    String clazz = databaseMeta.getDriverClass();
    if ( databaseMeta.getDatabaseInterface() != null ) {
      ds.setDriverClassLoader( databaseMeta.getDatabaseInterface().getClass().getClassLoader() );
    }
    ds.setDriverClassName( clazz );
  }

  private static void setCredentials( BasicDataSource ds, DatabaseMeta databaseMeta, String partitionId )
    throws HopDatabaseException {

    String userName = databaseMeta.environmentSubstitute( databaseMeta.getUsername() );
    String password = databaseMeta.environmentSubstitute( databaseMeta.getPassword() );
    password = Encr.decryptPasswordOptionallyEncrypted( password );

    ds.addConnectionProperty( "user", Const.NVL( userName, "" ) );
    ds.addConnectionProperty( "password", Const.NVL( password, "" ) );
  }

  @SuppressWarnings( "deprecation" )
  private static void setPoolProperties( BasicDataSource ds, Properties properties, int initialSize, int maxSize ) {
    ds.setInitialSize( initialSize );
    ds.setMaxActive( maxSize );

    String value = properties.getProperty( DEFAULT_AUTO_COMMIT );
    if ( !Utils.isEmpty( value ) ) {
      ds.setDefaultAutoCommit( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( DEFAULT_READ_ONLY );
    if ( !Utils.isEmpty( value ) ) {
      ds.setDefaultReadOnly( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( DEFAULT_TRANSACTION_ISOLATION );
    if ( !Utils.isEmpty( value ) ) {
      ds.setDefaultTransactionIsolation( Integer.valueOf( value ) );
    }

    value = properties.getProperty( DEFAULT_CATALOG );
    if ( !Utils.isEmpty( value ) ) {
      ds.setDefaultCatalog( value );
    }

    value = properties.getProperty( INITIAL_SIZE );
    if ( !Utils.isEmpty( value ) ) {
      ds.setInitialSize( Integer.valueOf( value ) );
    }

    value = properties.getProperty( MAX_ACTIVE );
    if ( !Utils.isEmpty( value ) ) {
      ds.setMaxActive( Integer.valueOf( value ) );
    }

    value = properties.getProperty( MAX_IDLE );
    if ( !Utils.isEmpty( value ) ) {
      ds.setMaxIdle( Integer.valueOf( value ) );
    }

    value = properties.getProperty( MIN_IDLE );
    if ( !Utils.isEmpty( value ) ) {
      ds.setMinIdle( Integer.valueOf( value ) );
    }

    value = properties.getProperty( MAX_WAIT );
    if ( !Utils.isEmpty( value ) ) {
      ds.setMaxWait( Long.valueOf( value ) );
    }

    value = properties.getProperty( VALIDATION_QUERY );
    if ( !Utils.isEmpty( value ) ) {
      ds.setValidationQuery( value );
    }

    value = properties.getProperty( TEST_ON_BORROW );
    if ( !Utils.isEmpty( value ) ) {
      ds.setTestOnBorrow( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( TEST_ON_RETURN );
    if ( !Utils.isEmpty( value ) ) {
      ds.setTestOnReturn( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( TEST_WHILE_IDLE );
    if ( !Utils.isEmpty( value ) ) {
      ds.setTestWhileIdle( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( TIME_BETWEEN_EVICTION_RUNS_MILLIS );
    if ( !Utils.isEmpty( value ) ) {
      ds.setTimeBetweenEvictionRunsMillis( Long.valueOf( value ) );
    }

    value = properties.getProperty( POOL_PREPARED_STATEMENTS );
    if ( !Utils.isEmpty( value ) ) {
      ds.setPoolPreparedStatements( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( MAX_OPEN_PREPARED_STATEMENTS );
    if ( !Utils.isEmpty( value ) ) {
      ds.setMaxOpenPreparedStatements( Integer.valueOf( value ) );
    }

    value = properties.getProperty( ACCESS_TO_UNDERLYING_CONNECTION_ALLOWED );
    if ( !Utils.isEmpty( value ) ) {
      ds.setAccessToUnderlyingConnectionAllowed( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( REMOVE_ABANDONED );
    if ( !Utils.isEmpty( value ) ) {
      ds.setRemoveAbandoned( Boolean.valueOf( value ) );
    }

    value = properties.getProperty( REMOVE_ABANDONED_TIMEOUT );
    if ( !Utils.isEmpty( value ) ) {
      ds.setRemoveAbandonedTimeout( Integer.valueOf( value ) );
    }

    value = properties.getProperty( LOG_ABANDONED );
    if ( !Utils.isEmpty( value ) ) {
      ds.setLogAbandoned( Boolean.valueOf( value ) );
    }

  }

  /**
   * This method verifies that it's possible to get connection fron a datasource
   *
   * @param ds
   * @throws HopDatabaseException
   */
  private static void testDataSource( DataSource ds ) throws HopDatabaseException {
    Connection conn = null;
    try {
      conn = ds.getConnection();
    } catch ( Throwable e ) {
      throw new HopDatabaseException( BaseMessages.getString( PKG,
          "Database.UnableToPreLoadConnectionToConnectionPool.Exception" ), e );
    } finally {
      DatabaseUtil.closeSilently( conn );
    }
  }

  /**
   * This methods adds a new data source to cache
   *
   * @param log
   * @param databaseMeta
   * @param partitionId
   * @param initialSize
   * @param maximumSize
   * @throws HopDatabaseException
   */
  private static void addPoolableDataSource( LogChannelInterface log, DatabaseMeta databaseMeta, String partitionId,
      int initialSize, int maximumSize ) throws HopDatabaseException {
    if ( log.isBasic() ) {
      log.logBasic( BaseMessages.getString( PKG, "Database.CreatingConnectionPool", databaseMeta.getName() ) );
    }

    BasicDataSource ds = new BasicDataSource();
    configureDataSource( ds, databaseMeta, partitionId, initialSize, maximumSize );
    // check if datasource is valid
    testDataSource( ds );
    // register data source
    dataSources.put( getDataSourceName( databaseMeta, partitionId ), ds );

    if ( log.isBasic() ) {
      log.logBasic( BaseMessages.getString( PKG, "Database.CreatedConnectionPool", databaseMeta.getName() ) );
    }
  }

  protected static String buildPoolName( DatabaseMeta dbMeta, String partitionId ) {
    return dbMeta.getName() + Const.NVL( dbMeta.getDatabaseName(), "" )
        + Const.NVL( dbMeta.getHostname(),  ""  ) + Const.NVL( dbMeta.getPort(),  ""  )
        + Const.NVL( partitionId, "" );
  }

}
