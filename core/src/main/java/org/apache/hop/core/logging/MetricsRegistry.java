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

package org.apache.hop.core.logging;


import org.apache.hop.core.metrics.IMetricsSnapshot;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This singleton will capture all the metrics coming from the various log channels based on the log channel ID.
 *
 * @author matt
 */
public class MetricsRegistry {
  private static MetricsRegistry registry = new MetricsRegistry();

  private Map<String, Map<String, IMetricsSnapshot>> snapshotMaps;
  private Map<String, Queue<IMetricsSnapshot>> snapshotLists;

  public static MetricsRegistry getInstance() {
    return registry;
  }

  private MetricsRegistry() {
    snapshotMaps = new ConcurrentHashMap<>();
    snapshotLists = new ConcurrentHashMap<>();
  }

  public void addSnapshot( ILogChannel logChannel, IMetricsSnapshot snapshot ) {
    IMetrics metric = snapshot.getMetric();
    String channelId = logChannel.getLogChannelId();
    switch ( metric.getType() ) {
      case START:
      case STOP:
        Queue<IMetricsSnapshot> list = getSnapshotList( channelId );
        list.add( snapshot );

        break;
      case MIN:
      case MAX:
      case SUM:
      case COUNT:
        Map<String, IMetricsSnapshot> map = getSnapshotMap( channelId );
        map.put( snapshot.getKey(), snapshot );

        break;
      default:
        break;
    }
  }

  public Map<String, Queue<IMetricsSnapshot>> getSnapshotLists() {
    return snapshotLists;
  }

  public Map<String, Map<String, IMetricsSnapshot>> getSnapshotMaps() {
    return snapshotMaps;
  }

  /**
   * Get the snapshot list for the given log channel ID. If no list is available, one is created (and stored).
   *
   * @param logChannelId The log channel to use.
   * @return an existing or a new metrics snapshot list.
   */
  public Queue<IMetricsSnapshot> getSnapshotList( String logChannelId ) {
    Queue<IMetricsSnapshot> list = snapshotLists.get( logChannelId );
    if ( list == null ) {
      list = new ConcurrentLinkedQueue<>();
      snapshotLists.put( logChannelId, list );
    }
    return list;

  }

  /**
   * Get the snapshot map for the given log channel ID. If no map is available, one is created (and stored).
   *
   * @param logChannelId The log channel to use.
   * @return an existing or a new metrics snapshot map.
   */
  public Map<String, IMetricsSnapshot> getSnapshotMap( String logChannelId ) {
    Map<String, IMetricsSnapshot> map = snapshotMaps.get( logChannelId );
    if ( map == null ) {
      map = new ConcurrentHashMap<>();
      snapshotMaps.put( logChannelId, map );
    }
    return map;
  }

  public void reset() {
    snapshotMaps.clear();
    snapshotLists.clear();
  }
}
