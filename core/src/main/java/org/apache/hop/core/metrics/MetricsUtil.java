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

package org.apache.hop.core.metrics;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.IMetrics;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.core.logging.MetricsRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class MetricsUtil {

  /**
   * Calculates the durations between the START and STOP snapshots for a given metric description
   *
   * @param logChannelId the id of the log channel to investigate
   * @param metricsCode  the metric code
   * @return the duration in ms
   */
  public static List<MetricsDuration> getDuration( String logChannelId, Metrics metric ) {
    List<MetricsDuration> durations = new ArrayList<>();

    Queue<IMetricsSnapshot> metrics = MetricsRegistry.getInstance().getSnapshotList( logChannelId );
    IMetricsSnapshot start = null;

    Iterator<IMetricsSnapshot> iterator = metrics.iterator();
    while ( iterator.hasNext() ) {
      IMetricsSnapshot snapshot = iterator.next();
      if ( snapshot.getMetric().equals( metric ) ) {
        if ( snapshot.getMetric().getType() == MetricsSnapshotType.START ) {
          if ( start != null ) {
            // We didn't find a stop for the previous start so add it with a null duration
            durations.add( new MetricsDuration( start.getDate(), snapshot.getMetric().getDescription(), snapshot
              .getSubject(), logChannelId, null ) );
          }
          start = snapshot;
        } else {
          long duration = snapshot.getDate().getTime() - start.getDate().getTime();
          durations.add( new MetricsDuration( start.getDate(), snapshot.getMetric().getDescription(), snapshot
            .getSubject(), logChannelId, duration ) );
          start = null;
        }
      }
    }

    // Now aggregate even further, calculate total times...
    //
    Map<String, MetricsDuration> map = new HashMap<>();
    for ( MetricsDuration duration : durations ) {
      String key =
        duration.getSubject() == null ? duration.getDescription() : duration.getDescription()
          + " / " + duration.getSubject();
      MetricsDuration agg = map.get( key );
      if ( agg == null ) {
        map.put( key, duration );
      } else {
        agg.setDuration( agg.getDuration() + duration.getDuration() );
      }
    }

    // If we already have

    return new ArrayList<>( map.values() );
  }

  public static List<MetricsDuration> getAllDurations( String parentLogChannelId ) {
    List<MetricsDuration> durations = new ArrayList<>();

    List<String> logChannelIds = LoggingRegistry.getInstance().getLogChannelChildren( parentLogChannelId );
    for ( String logChannelId : logChannelIds ) {
      ILoggingObject object = LoggingRegistry.getInstance().getLoggingObject( logChannelId );
      if ( object != null ) {
        durations.addAll( getDurations( logChannelId ) );
      }
    }

    return durations;
  }

  /**
   * Calculates the durations between the START and STOP snapshots per metric description and subject (if any)
   *
   * @param logChannelId the id of the log channel to investigate
   * @return the duration in ms
   */
  public static List<MetricsDuration> getDurations( String logChannelId ) {
    Map<String, IMetricsSnapshot> last = new HashMap<>();
    Map<String, MetricsDuration> map = new HashMap<>();

    Queue<IMetricsSnapshot> metrics = MetricsRegistry.getInstance().getSnapshotList( logChannelId );

    Iterator<IMetricsSnapshot> iterator = metrics.iterator();
    while ( iterator.hasNext() ) {
      IMetricsSnapshot snapshot = iterator.next();

      // Do we have a start point in the map?
      //
      String key =
        snapshot.getMetric().getDescription()
          + ( snapshot.getSubject() == null ? "" : ( " - " + snapshot.getSubject() ) );
      IMetricsSnapshot lastSnapshot = last.get( key );
      if ( lastSnapshot == null ) {
        lastSnapshot = snapshot;
        last.put( key, lastSnapshot );
      } else {
        // If we have a START-STOP range, calculate the duration and add it to the duration map...
        //
        IMetrics metric = lastSnapshot.getMetric();
        if ( metric.getType() == MetricsSnapshotType.START
          && snapshot.getMetric().getType() == MetricsSnapshotType.STOP ) {
          long extraDuration = snapshot.getDate().getTime() - lastSnapshot.getDate().getTime();

          MetricsDuration metricsDuration = map.get( key );
          if ( metricsDuration == null ) {
            metricsDuration =
              new MetricsDuration(
                lastSnapshot.getDate(), metric.getDescription(), lastSnapshot.getSubject(), logChannelId,
                extraDuration );
          } else {
            metricsDuration.setDuration( metricsDuration.getDuration() + extraDuration );
            metricsDuration.incrementCount();
            if ( metricsDuration.getEndDate().getTime() < snapshot.getDate().getTime() ) {
              metricsDuration.setEndDate( snapshot.getDate() );
            }
          }
          map.put( key, metricsDuration );
        }
      }
    }

    return new ArrayList<>( map.values() );
  }

  public static List<IMetricsSnapshot> getResultsList( Metrics metric ) {
    List<IMetricsSnapshot> snapshots = new ArrayList<>();

    Map<String, Map<String, IMetricsSnapshot>> snapshotMaps =
      MetricsRegistry.getInstance().getSnapshotMaps();
    Iterator<Map<String, IMetricsSnapshot>> mapsIterator = snapshotMaps.values().iterator();
    while ( mapsIterator.hasNext() ) {
      Map<String, IMetricsSnapshot> map = mapsIterator.next();
      Iterator<IMetricsSnapshot> snapshotIterator = map.values().iterator();
      while ( snapshotIterator.hasNext() ) {
        IMetricsSnapshot snapshot = snapshotIterator.next();
        if ( snapshot.getMetric().equals( metric ) ) {
          snapshots.add( snapshot );
        }
      }
    }

    return snapshots;
  }

  public static Long getResult( Metrics metric ) {
    Map<String, Map<String, IMetricsSnapshot>> snapshotMaps =
      MetricsRegistry.getInstance().getSnapshotMaps();
    Iterator<Map<String, IMetricsSnapshot>> mapsIterator = snapshotMaps.values().iterator();
    while ( mapsIterator.hasNext() ) {
      Map<String, IMetricsSnapshot> map = mapsIterator.next();
      Iterator<IMetricsSnapshot> snapshotIterator = map.values().iterator();
      while ( snapshotIterator.hasNext() ) {
        IMetricsSnapshot snapshot = snapshotIterator.next();
        if ( snapshot.getMetric().equals( metric ) ) {
          return snapshot.getValue();
        }
      }
    }

    return null;
  }

}
