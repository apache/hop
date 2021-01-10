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

package org.apache.hop.pipeline.engine;

import org.apache.hop.pipeline.performance.PerformanceSnapShot;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes the metrics you can get from an execution engine
 */
public class EngineMetrics {

  private Date startDate;
  private Date endDate;

  private List<IEngineComponent> components;
  private Map<IEngineComponent, Map<IEngineMetric, Long>> componentMetricsMap;
  private Map<IEngineComponent, String> componentStatusMap;
  private Map<IEngineComponent, String> componentSpeedMap;
  private Map<IEngineComponent, Boolean> componentRunningMap;

  private Map<IEngineComponent, List<PerformanceSnapShot>> componentPerformanceSnapshots;


  public EngineMetrics() {
    components = new ArrayList<>();
    componentMetricsMap = new HashMap<>();
    componentStatusMap = new HashMap<>();
    componentSpeedMap = new HashMap<>();
    componentRunningMap = new HashMap<>();
    componentPerformanceSnapshots = new HashMap<>();
  }

  /**
   * Get a list of the used metrics
   *
   * @return The list of used metrics
   */
  public Set<IEngineMetric> getMetricsList() {
    Set<IEngineMetric> set = new HashSet<>();
    for ( Map<IEngineMetric, Long> metricsMap : componentMetricsMap.values() ) {
      for ( IEngineMetric metric : metricsMap.keySet() ) {
        set.add( metric );
      }
    }
    return set;
  }

  public void addComponent( IEngineComponent component ) {
    components.add( component );
  }

  /**
   * @param component The component to set a metric for (e.g. transform name)
   * @param metric    The metric (e.g. input, output, ...)
   * @param amount    the metric amount
   */
  public void setComponentMetric( IEngineComponent component, IEngineMetric metric, Long amount ) {
    if ( component == null || metric == null ) {
      throw new RuntimeException( "Please provide a component and a metric to set" );
    }
    Map<IEngineMetric, Long> metricsMap = componentMetricsMap.get( component );
    if ( metricsMap == null ) {
      metricsMap = new HashMap<>();
      componentMetricsMap.put( component, metricsMap );
    }
    metricsMap.put( metric, amount );
  }

  public void setComponentStatus( IEngineComponent component, String status ) {
    componentStatusMap.put( component, status );
  }

  public void setComponentSpeed( IEngineComponent component, String status ) {
    componentSpeedMap.put( component, status );
  }

  public void setComponentRunning( IEngineComponent component, Boolean running ) {
    componentRunningMap.put( component, running );
  }


  /**
   * Retrieve the amount for a specific metric
   *
   * @param component The component of the metric (e.g. transform name)
   * @param metric    The metric (e.g. input, output, ...)
   * @return the metric amount or null if nothing was found
   */
  public Long getComponentMetric( IEngineComponent component, IEngineMetric metric ) {
    if ( component == null || metric == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to retrieve" );
    }
    Map<IEngineMetric, Long> metricsMap = componentMetricsMap.get( component );
    if ( metricsMap == null ) {
      return null;
    }
    return metricsMap.get( metric );
  }

  /**
   * Increment the amount for a specific metric (+1).  If the metric didn't exist create it and set it to 1.
   *
   * @param component The component of the metric (e.g. transform name)
   * @param metric    The metric (e.g. input, output, ...)
   */
  public void incrementComponentMetric( IEngineComponent component, IEngineMetric metric ) {
    if ( component == null || metric == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to increment" );
    }
    Long amount = getComponentMetric( component, metric );
    if ( amount == null ) {
      setComponentMetric( component, metric, 1L );
    } else {
      setComponentMetric( component, metric, amount + 1 );
    }
  }

  /**
   * Remove the amount for a specific metric
   *
   * @param component The component of the metric (e.g. transform name)
   * @param metric    The name of the metric (e.g. input, output, ...)
   * @return the metric amount or null if nothing stored in the first place
   */
  public Long removeComponentMetric( IEngineComponent component, IEngineMetric metric ) {
    if ( component == null || metric == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to remove" );
    }
    Map<IEngineMetric, Long> metricsMap = componentMetricsMap.get( component );
    if ( metricsMap == null ) {
      return null;
    }
    return metricsMap.remove( metric );
  }

  public void addCompomentPerformanceSnapShot(IEngineComponent component, PerformanceSnapShot snapShot) {
    if ( component == null || snapShot == null ) {
      throw new RuntimeException( "Please provide a component and a snapshot to add" );
    }
    List<PerformanceSnapShot> snapShots = componentPerformanceSnapshots.get( component );
    if (snapShots==null) {
      snapShots = new ArrayList<>(  );
      componentPerformanceSnapshots.put(component, snapShots);
    }
    snapShots.add(snapShot);
  }

  /**
   * Gets startDate
   *
   * @return value of startDate
   */
  public Date getStartDate() {
    return startDate;
  }

  /**
   * @param startDate The startDate to set
   */
  public void setStartDate( Date startDate ) {
    this.startDate = startDate;
  }

  /**
   * Gets endDate
   *
   * @return value of endDate
   */
  public Date getEndDate() {
    return endDate;
  }

  /**
   * @param endDate The endDate to set
   */
  public void setEndDate( Date endDate ) {
    this.endDate = endDate;
  }

  /**
   * Gets componentMetricsMap
   *
   * @return value of componentMetricsMap
   */
  public Map<IEngineComponent, Map<IEngineMetric, Long>> getComponentMetricsMap() {
    return componentMetricsMap;
  }

  /**
   * @param componentMetricsMap The componentMetricsMap to set
   */
  public void setComponentMetricsMap( Map<IEngineComponent, Map<IEngineMetric, Long>> componentMetricsMap ) {
    this.componentMetricsMap = componentMetricsMap;
  }

  /**
   * Gets componentStatusMap
   *
   * @return value of componentStatusMap
   */
  public Map<IEngineComponent, String> getComponentStatusMap() {
    return componentStatusMap;
  }

  /**
   * @param componentStatusMap The componentStatusMap to set
   */
  public void setComponentStatusMap( Map<IEngineComponent, String> componentStatusMap ) {
    this.componentStatusMap = componentStatusMap;
  }

  /**
   * Gets componentSpeedMap
   *
   * @return value of componentSpeedMap
   */
  public Map<IEngineComponent, String> getComponentSpeedMap() {
    return componentSpeedMap;
  }

  /**
   * @param componentSpeedMap The componentSpeedMap to set
   */
  public void setComponentSpeedMap( Map<IEngineComponent, String> componentSpeedMap ) {
    this.componentSpeedMap = componentSpeedMap;
  }

  /**
   * Gets componentRunningMap
   *
   * @return value of componentRunningMap
   */
  public Map<IEngineComponent, Boolean> getComponentRunningMap() {
    return componentRunningMap;
  }

  /**
   * @param componentRunningMap The componentRunningMap to set
   */
  public void setComponentRunningMap( Map<IEngineComponent, Boolean> componentRunningMap ) {
    this.componentRunningMap = componentRunningMap;
  }

  /**
   * Gets components
   *
   * @return value of components
   */
  public List<IEngineComponent> getComponents() {
    return components;
  }

  /**
   * @param components The components to set
   */
  public void setComponents( List<IEngineComponent> components ) {
    this.components = components;
  }

  /**
   * Gets componentPerformanceSnapshots
   *
   * @return value of componentPerformanceSnapshots
   */
  public Map<IEngineComponent, List<PerformanceSnapShot>> getComponentPerformanceSnapshots() {
    return componentPerformanceSnapshots;
  }

  /**
   * @param componentPerformanceSnapshots The componentPerformanceSnapshots to set
   */
  public void setComponentPerformanceSnapshots(
    Map<IEngineComponent, List<PerformanceSnapShot>> componentPerformanceSnapshots ) {
    this.componentPerformanceSnapshots = componentPerformanceSnapshots;
  }
}
