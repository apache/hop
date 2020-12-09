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

package org.apache.hop.core.database.map;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabaseTransaction;
import org.apache.hop.core.util.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class contains a map between on the one hand
 * <p/>
 * the pipeline name/thread the partition ID the connection group
 * <p/>
 * And on the other hand
 * <p/>
 * The database connection The number of times it was opened
 *
 * @author Matt
 */
public class DatabaseConnectionMap {
  private final ConcurrentMap<String, Database> map;
  private final AtomicInteger transactionId;
  private final Map<String, List<IDatabaseTransaction>> transactionListenersMap;

  private static final DatabaseConnectionMap connectionMap = new DatabaseConnectionMap();

  public static synchronized DatabaseConnectionMap getInstance() {
    return connectionMap;
  }

  private DatabaseConnectionMap() {
    map = new ConcurrentHashMap<>();
    transactionId = new AtomicInteger( 0 );
    transactionListenersMap = new HashMap<>();
  }

  /**
   * Tries to obtain an existing <tt>Database</tt> instance for specified parameters. If none is found, then maps the
   * key's value to <tt>database</tt>. Similarly to {@linkplain ConcurrentHashMap#putIfAbsent(Object, Object)} returns
   * <tt>null</tt> if there was no value for the specified key and they mapped value otherwise.
   *
   * @param connectionGroup connection group
   * @param partitionID     partition's id
   * @param database        database
   * @return <tt>null</tt> or previous value
   */
  public Database getOrStoreIfAbsent( String connectionGroup, String partitionID, Database database ) {
    String key = createEntryKey( connectionGroup, partitionID, database );
    return map.putIfAbsent( key, database );
  }

  public void removeConnection( String connectionGroup, String partitionID, Database database ) {
    String key = createEntryKey( connectionGroup, partitionID, database );
    map.remove( key );
  }

  /**
   * @deprecated use {@linkplain #getOrStoreIfAbsent(String, String, Database)} instead
   */
  @Deprecated
  public synchronized void storeDatabase( String connectionGroup, String partitionID, Database database ) {
    String key = createEntryKey( connectionGroup, partitionID, database );
    map.put( key, database );
  }

  /**
   * @deprecated use {@linkplain #getOrStoreIfAbsent(String, String, Database)} instead
   */
  @Deprecated
  public synchronized Database getDatabase( String connectionGroup, String partitionID, Database database ) {
    String key = createEntryKey( connectionGroup, partitionID, database );
    return map.get( key );
  }

  public static String createEntryKey( String connectionGroup, String partitionID, Database database ) {
    StringBuilder key = new StringBuilder( connectionGroup );

    key.append( ':' ).append( database.getDatabaseMeta().getName() );
    if ( !Utils.isEmpty( partitionID ) ) {
      key.append( ':' ).append( partitionID );
    }

    return key.toString();
  }

  public Map<String, Database> getMap() {
    return map;
  }

  public String getNextTransactionId() {
    return Integer.toString( transactionId.incrementAndGet() );
  }

  public void addPipelineactionListener( String transactionId, IDatabaseTransaction listener ) {
    List<IDatabaseTransaction> transactionListeners = getTransactionListeners( transactionId );
    transactionListeners.add( listener );
  }

  public void removeTransactionListener( String transactionId, IDatabaseTransaction listener ) {
    List<IDatabaseTransaction> transactionListeners = getTransactionListeners( transactionId );
    transactionListeners.remove( listener );
  }

  public List<IDatabaseTransaction> getTransactionListeners( String transactionId ) {
    List<IDatabaseTransaction> transactionListeners = transactionListenersMap.get( transactionId );
    if ( transactionListeners == null ) {
      transactionListeners = new ArrayList<>();
      transactionListenersMap.put( transactionId, transactionListeners );
    }
    return transactionListeners;
  }

  public void removeTransactionListeners( String transactionId ) {
    transactionListenersMap.remove( transactionId );
  }
}
