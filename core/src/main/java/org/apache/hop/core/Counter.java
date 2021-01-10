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

import java.util.concurrent.locks.ReentrantLock;

/**
 * Is used to keep the state of sequences / counters throughout a single session of a Pipeline, but
 * across transforms.
 *
 * @author Matt
 * @since 13-05-2003
 */
public class Counter {
  private long counter;
  private long start;
  private long increment;
  private long maximum;
  private boolean loop;

  private ReentrantLock lock;

  public Counter() {
    start = 1L;
    increment = 1L;
    maximum = 0L;
    loop = false;
    counter = start;
    lock = new ReentrantLock();
  }

  public Counter(long start) {
    this();
    this.start = start;
    counter = start;
    lock = new ReentrantLock();
  }

  public Counter(long start, long increment) {
    this(start);
    this.increment = increment;
  }

  public Counter(long start, long increment, long maximum) {
    this(start, increment);
    this.loop = true;
    this.maximum = maximum;
  }

  /** @return Returns the counter. */
  public long getCounter() {
    return counter;
  }

  /** @return Returns the increment. */
  public long getIncrement() {
    return increment;
  }

  /** @return Returns the maximum. */
  public long getMaximum() {
    return maximum;
  }

  /** @return Returns the start. */
  public long getStart() {
    return start;
  }

  /** @return Returns the loop. */
  public boolean isLoop() {
    return loop;
  }

  /** @param counter The counter to set. */
  public void setCounter(long counter) {
    this.counter = counter;
  }

  /** @param increment The increment to set. */
  public void setIncrement(long increment) {
    this.increment = increment;
  }

  /** @param loop The loop to set. */
  public void setLoop(boolean loop) {
    this.loop = loop;
  }

  /** @param maximum The maximum to set. */
  public void setMaximum(long maximum) {
    this.maximum = maximum;
  }

  public long getAndNext() {

    lock.lock();
    try {
      long value = counter;
      long nextValue = counter + increment;

      if (loop) {
        if (increment < 0) {
          if (maximum < start && nextValue < maximum) {
            nextValue = start;
          }
        } else if (increment > 0) {
          if (maximum > start && nextValue > maximum) {
            nextValue = start;
          }
        }
      }
      counter = nextValue;

      return value;
    } finally {
      lock.unlock();
    }
  }
}
