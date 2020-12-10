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

package org.apache.hop.core;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Contains the base IRowSet class to help implement IRowSet variants.
 *
 * @author Matt
 * @since 22-01-2010
 */
abstract class BaseRowSet implements Comparable<IRowSet>, IRowSet {
  protected IRowMeta rowMeta;

  protected AtomicBoolean done;
  protected volatile String originTransformName;
  protected AtomicInteger originTransformCopy;
  protected volatile String destinationTransformName;
  protected AtomicInteger destinationTransformCopy;

  protected volatile String remoteHopServerName;
  private ReadWriteLock lock;

  public BaseRowSet() {
    // not done putting data into this IRowSet
    done = new AtomicBoolean( false );

    originTransformCopy = new AtomicInteger( 0 );
    destinationTransformCopy = new AtomicInteger( 0 );
    lock = new ReentrantReadWriteLock();
  }

  /**
   * Compares using the target transforms and copy, not the source.
   * That way, re-partitioning is always done in the same way.
   */
  @Override
  public int compareTo( IRowSet rowSet ) {
    lock.readLock().lock();
    String target;

    try {
      target = remoteHopServerName + "." + destinationTransformName + "." + destinationTransformCopy.intValue();
    } finally {
      lock.readLock().unlock();
    }

    String comp =
      rowSet.getRemoteHopServerName()
        + "." + rowSet.getDestinationTransformName() + "." + rowSet.getDestinationTransformCopy();

    return target.compareTo( comp );
  }

  public boolean equals( BaseRowSet rowSet ) {
    return compareTo( rowSet ) == 0;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#putRow(org.apache.hop.core.row.IRowMeta, java.lang.Object[])
   */
  @Override
  public abstract boolean putRow( IRowMeta rowMeta, Object[] rowData );

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#putRowWait(org.apache.hop.core.row.IRowMeta, java.lang.Object[],
   * long, java.util.concurrent.TimeUnit)
   */
  @Override
  public abstract boolean putRowWait( IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu );

  // default getRow with wait time = 100ms
  //
  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRow()
   */
  @Override
  public abstract Object[] getRow();

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRowImmediate()
   */
  @Override
  public abstract Object[] getRowImmediate();

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRowWait(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public abstract Object[] getRowWait( long timeout, TimeUnit tu );

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#setDone()
   */
  @Override
  public void setDone() {
    done.set( true );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#isDone()
   */
  @Override
  public boolean isDone() {
    return done.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getOriginTransformName()
   */
  @Override
  public String getOriginTransformName() {
    return originTransformName;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getOriginTransformCopy()
   */
  @Override
  public int getOriginTransformCopy() {
    return originTransformCopy.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getDestinationTransformName()
   */
  @Override
  public String getDestinationTransformName() {
    return destinationTransformName;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getDestinationTransformCopy()
   */
  @Override
  public int getDestinationTransformCopy() {
    return destinationTransformCopy.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getName()
   */
  @Override
  public String getName() {
    return toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#size()
   */
  @Override
  public abstract int size();

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#setThreadNameFromToCopy(java.lang.String, int, java.lang.String, int)
   */
  @Override
  public void setThreadNameFromToCopy( String from, int fromCopy, String to, int toCopy ) {

    lock.writeLock().lock();
    try {
      originTransformName = from;
      originTransformCopy.set( fromCopy );

      destinationTransformName = to;
      destinationTransformCopy.set( toCopy );
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder str;

    lock.readLock().lock();
    try {
      str = new StringBuilder( Const.NVL(originTransformName, "?") )
        .append( "." )
        .append( originTransformCopy )
        .append( " - " )
        .append( Const.NVL(destinationTransformName, "?") )
        .append( "." )
        .append( destinationTransformCopy );

      if ( !Utils.isEmpty( remoteHopServerName ) ) {
        str.append( " (" )
          .append( remoteHopServerName )
          .append( ")" );
      }
    } finally {
      lock.readLock().unlock();
    }

    return str.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRowMeta()
   */
  @Override
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#setRowMeta(org.apache.hop.core.row.IRowMeta)
   */
  @Override
  public void setRowMeta( IRowMeta rowMeta ) {
    this.rowMeta = rowMeta;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRemoteHopServerName()
   */
  @Override
  public String getRemoteHopServerName() {
    return remoteHopServerName;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#setRemoteHopServerName(java.lang.String)
   */
  @Override
  public void setRemoteHopServerName( String remoteHopServerName ) {
    this.remoteHopServerName = remoteHopServerName;
  }

  /**
   * By default we don't report blocking, only for monitored pipelines.
   *
   * @return true if this row set is blocking on reading or writing.
   */
  @Override
  public boolean isBlocking() {
    return false;
  }

}
