/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.www.cache;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.Const;
import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class HopServerStatusCache implements Cache {

  public static final String HOP_SERVER_STATUS_CACHE = "HOP_SERVER_CACHE";

  /**
   * Switching the thread launched to be daemon otherwise it blocks the Hop server shutdown
   */
  private final ScheduledExecutorService removeService = Executors.newSingleThreadScheduledExecutor(
    r -> {
      Thread t = Executors.defaultThreadFactory().newThread( r );
      t.setDaemon( true );
      t.setName( HopServerStatusCache.class.getSimpleName() );
      return t;
    } );

  private final Map<String, CachedItem> cachedMap = new ConcurrentHashMap<>();

  private static HopServerStatusCache instance = null;

  private int period = 0;

  private TimeUnit timeUnit = null;

  public static synchronized HopServerStatusCache getInstance() {
    if ( instance == null ) {
      instance = new HopServerStatusCache();
    }
    return instance;
  }

  private HopServerStatusCache() {
    period = Integer.parseInt( Const.getEnvironmentVariable( "HOP_SERVER_CLEAR_PERIOD", "1" ) );
    timeUnit = TimeUnit.valueOf( Const.getEnvironmentVariable( "HOP_SERVER_CLEAR_TIMEUNIT", "DAYS" ) );

    removeService.scheduleAtFixedRate( this::clear, 1, 1, TimeUnit.DAYS );
  }


  public void put( String logId, String cacheString, int from ) {
    String randomPref = UUID.randomUUID().toString();
    File file = null;
    try {
      file = File.createTempFile( randomPref, null );
      file.deleteOnExit();
      Files.write( file.toPath(), cacheString.getBytes( Const.XML_ENCODING ) );
      CachedItem item = new CachedItem( file, from );
      if ( ( item = cachedMap.put( logId, item ) ) != null ) {
        removeTask( item.getFile() );
      }
    } catch ( Exception e ) {
      cachedMap.remove( logId );
      if ( file != null ) {
        file.delete();
      }
    }
  }


  public byte[] get( String logId, int from ) {
    CachedItem item = null;
    try {
      item = cachedMap.get( logId );
      if ( item == null || item.getFrom() != from ) {
        return null;
      }

      synchronized ( item.getFile() ) {
        return Files.readAllBytes( item.getFile().toPath() );
      }


    } catch ( Exception e ) {
      cachedMap.remove( logId );
      if ( item != null ) {
        removeTask( item.getFile() );
      }
    }
    return null;
  }

  public void remove( String id ) {
    CachedItem item = cachedMap.remove( id );
    if ( item != null ) {
      removeTask( item.getFile() );
    }
  }

  private void removeTask( File file ) {
    removeService.execute( () -> removeFile( file ) );
  }

  private void removeFile( File file ) {
    synchronized ( file ) {
      if ( file.exists() ) {
        FileUtils.deleteQuietly( file );
      }
    }
  }

  @VisibleForTesting
  Map<String, CachedItem> getMap() {
    return cachedMap;
  }

  @Override
  public Object read( Object key ) throws CacheException {
    return cachedMap.get( key );
  }

  @Override
  public Object get( Object key ) throws CacheException {
    return cachedMap.get( key );
  }

  @Override
  public void put( Object key, Object value ) throws CacheException {
    cachedMap.put( (String) key, (CachedItem) value );
  }

  @Override
  public void update( Object key, Object value ) throws CacheException {
    put( (String) key, (CachedItem) value );
  }

  @Override
  public void remove( Object key ) throws CacheException {
    remove( (String) key );
  }

  @Override
  public void clear() throws CacheException {
    cachedMap.forEach( ( k, v ) -> {
      if ( LocalDate.now().isAfter( v.getExceedTime() ) ) {
        remove( k );
      }
    } );
  }

  @Override
  public void destroy() throws CacheException {
    clear();
  }

  @Override
  public Map toMap() {
    return cachedMap;
  }

  @Override
  public void lock( Object key ) throws CacheException {
  }

  @Override
  public void unlock( Object key ) throws CacheException {
  }

  @Override public long nextTimestamp() {
    return 0;
  }

  @Override public int getTimeout() {
    return 0;
  }

  @Override public String getRegionName() {
    return null;
  }

  @Override public long getSizeInMemory() {
    return 0;
  }

  @Override public long getElementCountInMemory() {
    return 0;
  }

  @Override public long getElementCountOnDisk() {
    return 0;
  }
}
