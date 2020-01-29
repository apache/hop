package org.apache.hop.trans.engine;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes the metrics you can get from an execution engine
 */
public class EngineMetrics {

  private Date startDate;
  private Date endDate;

  private Map<String, Map<String, Long>> componentMetricsMap;

  public EngineMetrics() {
    componentMetricsMap = new HashMap<>();
  }

  /**
   * @param component The component to set a metric for (e.g. step name)
   * @param name      The name of the metric (e.g. input, output, ...)
   * @param amount    the metric amount
   */
  public void setComponentMetric( String component, String name, Long amount ) {
    if ( component == null || name == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to set" );
    }
    Map<String, Long> metricsMap = componentMetricsMap.get( component );
    if ( metricsMap == null ) {
      metricsMap = new HashMap<>();
      componentMetricsMap.put( component, metricsMap );
    }
    metricsMap.put( name, amount );
  }

  /**
   * Retrieve the amount for a specific metric
   *
   * @param component The component of the metric (e.g. step name)
   * @param name      The name of the metric (e.g. input, output, ...)
   * @return the metric amount or null if nothing was found
   */
  public Long getComponentMetric( String component, String name ) {
    if ( component == null || name == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to retrieve" );
    }
    Map<String, Long> metricsMap = componentMetricsMap.get( component );
    if ( metricsMap == null ) {
      return null;
    }
    return metricsMap.get( name );
  }

  /**
   * Increment the amount for a specific metric (+1).  If the metric didn't exist create it and set it to 1.
   *
   * @param component The component of the metric (e.g. step name)
   * @param name      The name of the metric (e.g. input, output, ...)
   */
  public void incrementComponentMetric( String component, String name ) {
    if ( component == null || name == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to increment" );
    }
    Long amount = getComponentMetric( component, name );
    if ( amount == null ) {
      setComponentMetric( component, name, 1L );
    } else {
      setComponentMetric( component, name, amount + 1 );
    }
  }

  /**
   * Remove the amount for a specific metric
   *
   * @param component The component of the metric (e.g. step name)
   * @param name      The name of the metric (e.g. input, output, ...)
   * @return the metric amount or null if nothing stored in the first place
   */
  public Long removeComponentMetric( String component, String name ) {
    if ( component == null || name == null ) {
      throw new RuntimeException( "Please provide a component and a name for the metric to remove" );
    }
    Map<String, Long> metricsMap = componentMetricsMap.get( component );
    if ( metricsMap == null ) {
      return null;
    }
    return metricsMap.remove( name );
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
  public Map<String, Map<String, Long>> getComponentMetricsMap() {
    return componentMetricsMap;
  }

  /**
   * @param componentMetricsMap The componentMetricsMap to set
   */
  public void setComponentMetricsMap( Map<String, Map<String, Long>> componentMetricsMap ) {
    this.componentMetricsMap = componentMetricsMap;
  }
}
