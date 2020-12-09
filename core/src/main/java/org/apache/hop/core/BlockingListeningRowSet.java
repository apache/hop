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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Contains a buffer of rows. Getting rows from the buffer or putting rows in the buffer is synchronized to allow
 * concurrent use of multiple Threads.
 * <p>
 * This class also monitors the idle state of a IRowSet
 *
 * @author Matt
 * @since 23-12-2010
 */
public class BlockingListeningRowSet extends BaseRowSet implements Comparable<IRowSet>, IRowSet {
  private BlockingQueue<Object[]> queArray;

  private AtomicBoolean blocking;

  /**
   * Create new non-blocking-queue with maxSize capacity.
   *
   * @param maxSize
   */
  public BlockingListeningRowSet( int maxSize ) {
    super();

    // create an empty queue
    queArray = new ArrayBlockingQueue<>( maxSize, false );
    blocking = new AtomicBoolean( false );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#putRow(org.apache.hop.core.row.IRowMeta, java.lang.Object[])
   */
  @Override
  public boolean putRow( IRowMeta rowMeta, Object[] rowData ) {
    return putRowWait( rowMeta, rowData, 100, TimeUnit.NANOSECONDS );
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#putRowWait(org.apache.hop.core.row.IRowMeta, java.lang.Object[],
   * long, java.util.concurrent.TimeUnit)
   */
  @Override
  public boolean putRowWait( IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu ) {
    this.rowMeta = rowMeta;
    try {
      blocking.set( true );
      boolean b = queArray.offer( rowData, time, tu );
      blocking.set( false );
      return b;
    } catch ( InterruptedException e ) {
      blocking.set( false );
      return false;
    } catch ( NullPointerException e ) {
      blocking.set( false );
      return false;
    }

  }

  // default getRow with wait time = 100ms
  //
  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRow()
   */
  @Override
  public Object[] getRow() {
    blocking.set( true );
    Object[] row = getRowWait( 100, TimeUnit.NANOSECONDS );
    blocking.set( false );
    return row;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRowImmediate()
   */
  @Override
  public Object[] getRowImmediate() {

    blocking.set( true );
    Object[] row = queArray.poll();
    blocking.set( false );
    return row;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#getRowWait(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public Object[] getRowWait( long timeout, TimeUnit tu ) {

    try {
      blocking.set( true );
      Object[] row = queArray.poll( timeout, tu );
      blocking.set( false );
      return row;
    } catch ( InterruptedException e ) {
      blocking.set( false );
      return null;
    }
  }

  @Override
  public int size() {
    return queArray.size();
  }

  /**
   * @return true if this row set is blocking.
   */
  @Override
  public boolean isBlocking() {
    return blocking.get();
  }

  @Override
  public void clear() {
    queArray.clear();
    done.set( false );
  }

}
