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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the counters for Hop, the pipelines, workflows, ...
 *
 * @author Matt
 * @since 17-apr-2005
 */
public class Counters {
  private static Counters counters = null;
  private Map<String, Counter> counterMap = null;

  private static final Object lock = new Object();

  private Counters() {
    counterMap = Collections.synchronizedMap(new HashMap<>());
  }

  public static final Counters getInstance() {
    if (counters != null) {
      return counters;
    }
    counters = new Counters();
    return counters;
  }

  public void setCounter(String name, Counter counter) {
    counterMap.put(name, counter);
  }

  public void clearCounter(String name) {
    counterMap.remove(name);
  }

  public void clear() {
    counterMap.clear();
  }

  public Counter getCounter(String name) {
    synchronized (lock) {
      Counter found = counterMap.get(name);
      return found;
    }
  }

  public synchronized Counter getOrUpdateCounter(String name, Counter counter) {
    synchronized (counterMap) {
      Counter found = counterMap.get(name);
      if (found == null) {
        found = counter;
        counterMap.put(name, counter);
      }
      return found;
    }
  }

  public Counter removeCounter(String name) {
    synchronized (lock) {
      return counterMap.remove(name);
    }
  }
}
