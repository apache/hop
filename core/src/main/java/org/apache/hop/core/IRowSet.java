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

import java.util.concurrent.TimeUnit;

public interface IRowSet {

  /**
   * Offer a row of data to this rowset providing for the description (metadata) of the row. If the buffer is full, wait
   * (block) for a small period of time.
   *
   * @param rowMeta The description of the row data
   * @param rowData the row of data
   * @return true if the row was successfully added to the rowset and false if this buffer was full.
   */
  boolean putRow( IRowMeta rowMeta, Object[] rowData );

  /**
   * Offer a row of data to this rowset providing for the description (metadata) of the row. If the buffer is full, wait
   * (block) for a period of time defined in this call.
   *
   * @param rowMeta The description of the row data
   * @param rowData the row of data
   * @param time    The number of units of time
   * @param tu      The unit of time to use
   * @return true if the row was successfully added to the rowset and false if this buffer was full.
   */
  boolean putRowWait( IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu );

  /**
   * Get a row from the input buffer, it blocks for a short period until a new row becomes available. Otherwise, it
   * returns null.
   *
   * @return a row of data or null if no row is available.
   */
  Object[] getRow();

  /**
   * Get the first row in the list immediately.
   *
   * @return a row of data or null if no row is available.
   */
  Object[] getRowImmediate();

  /**
   * get the first row in the list immediately if it is available or wait until timeout
   *
   * @return a row of data or null if no row is available.
   */
  Object[] getRowWait( long timeout, TimeUnit tu );

  /**
   * @return Set indication that there is no more input
   */
  void setDone();

  /**
   * @return Returns true if there is no more input and vice versa
   */
  boolean isDone();

  /**
   * @return Returns the origin transform name.
   */
  String getOriginTransformName();

  /**
   * @return Returns the origin transform copy.
   */
  int getOriginTransformCopy();

  /**
   * @return Returns the destination transform name.
   */
  String getDestinationTransformName();

  /**
   * @return Returns the destination transform copy.
   */
  int getDestinationTransformCopy();

  String getName();

  /**
   * @return Return the size (or max capacity) of the IRowSet
   */
  int size();

  /**
   * This method is used only in Pipeline.java when created IRowSet at line 333. Don't need any synchronization on this
   * method
   */
  void setThreadNameFromToCopy( String from, int fromCopy, String to, int toCopy );

  /**
   * @return the rowMeta
   */
  IRowMeta getRowMeta();

  /**
   * @param rowMeta the rowMeta to set
   */
  void setRowMeta( IRowMeta rowMeta );

  /**
   * @return the targetHopServer
   */
  String getRemoteHopServerName();

  /**
   * @param remoteHopServerName the remote hop server to set
   */
  void setRemoteHopServerName( String remoteHopServerName );

  /**
   * @return true if this row set is blocking.
   */
  boolean isBlocking();

  /**
   * Clear this rowset: remove all rows and remove the "done" flag.
   */
  void clear();
}
