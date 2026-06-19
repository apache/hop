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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.jspecify.annotations.NonNull;

/** Contains the base IRowSet class to help implement IRowSet variants. */
abstract class BaseRowSet implements Comparable<IRowSet>, IRowSet {
  @Getter @Setter protected IRowMeta rowMeta;

  protected AtomicBoolean done;
  @Getter protected volatile String originTransformName;
  protected AtomicInteger originTransformCopy;
  @Getter protected volatile String destinationTransformName;
  protected AtomicInteger destinationTransformCopy;
  @Getter @Setter protected volatile String remoteHopServerName;
  private final ReadWriteLock lock;

  protected BaseRowSet() {
    // not done putting data into this IRowSet
    done = new AtomicBoolean(false);

    originTransformCopy = new AtomicInteger(0);
    destinationTransformCopy = new AtomicInteger(0);
    lock = new ReentrantReadWriteLock();
  }

  /**
   * Compares using the target transforms and copy, not the source. That way, re-partitioning is
   * always done in the same way.
   */
  @Override
  public int compareTo(@NonNull IRowSet rowSet) {
    lock.readLock().lock();
    String target;

    try {
      target =
          remoteHopServerName
              + "."
              + destinationTransformName
              + "."
              + destinationTransformCopy.intValue();
    } finally {
      lock.readLock().unlock();
    }

    String comp =
        rowSet.getRemoteHopServerName()
            + "."
            + rowSet.getDestinationTransformName()
            + "."
            + rowSet.getDestinationTransformCopy();

    return target.compareTo(comp);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.RowSetInterface#setDone()
   */
  @Override
  public void setDone() {
    done.set(true);
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
   * @see org.apache.hop.core.RowSetInterface#getOriginTransformCopy()
   */
  @Override
  public int getOriginTransformCopy() {
    return originTransformCopy.get();
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
   * @see org.apache.hop.core.RowSetInterface#setThreadNameFromToCopy(java.lang.String, int, java.lang.String, int)
   */
  @Override
  public void setThreadNameFromToCopy(String from, int fromCopy, String to, int toCopy) {

    lock.writeLock().lock();
    try {
      originTransformName = from;
      originTransformCopy.set(fromCopy);

      destinationTransformName = to;
      destinationTransformCopy.set(toCopy);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder str;

    lock.readLock().lock();
    try {
      str =
          new StringBuilder(Const.NVL(originTransformName, "?"))
              .append(".")
              .append(originTransformCopy)
              .append(" - ")
              .append(Const.NVL(destinationTransformName, "?"))
              .append(".")
              .append(destinationTransformCopy);

      if (!Utils.isEmpty(remoteHopServerName)) {
        str.append(" (").append(remoteHopServerName).append(")");
      }
    } finally {
      lock.readLock().unlock();
    }

    return str.toString();
  }

  /**
   * By default, we don't report blocking, only for monitored pipelines.
   *
   * @return true if this row set is blocking on reading or writing.
   */
  @Override
  public boolean isBlocking() {
    return false;
  }
}
