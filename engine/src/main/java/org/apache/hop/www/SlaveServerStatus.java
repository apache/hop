/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.www;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

public class SlaveServerStatus {
  public static final String XML_TAG = "serverstatus";

  private String statusDescription;
  private String errorDescription;

  private List<SlaveServerPipelineStatus> pipelineStatusList;
  private List<SlaveServerJobStatus> jobStatusList;

  private long memoryFree;
  private long memoryTotal;

  private int cpuCores;
  private long cpuProcessTime;

  private long uptime;

  private int threadCount;

  private double loadAvg;

  private String osName;

  private String osVersion;

  private String osArchitecture;

  public SlaveServerStatus() {
    pipelineStatusList = new ArrayList<SlaveServerPipelineStatus>();
    jobStatusList = new ArrayList<SlaveServerJobStatus>();
  }

  public SlaveServerStatus( String statusDescription ) {
    this();
    this.statusDescription = statusDescription;
  }

  /**
   * @param statusDescription
   * @param pipelineStatusList
   * @param jobStatusList
   */
  public SlaveServerStatus( String statusDescription, List<SlaveServerPipelineStatus> pipelineStatusList,
                            List<SlaveServerJobStatus> jobStatusList ) {
    this.statusDescription = statusDescription;
    this.pipelineStatusList = pipelineStatusList;
    this.jobStatusList = jobStatusList;
  }

  public String getXML() throws HopException {
    StringBuilder xml = new StringBuilder();

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );
    xml.append( XMLHandler.addTagValue( "statusdesc", statusDescription ) );

    xml.append( XMLHandler.addTagValue( "memory_free", memoryFree ) );
    xml.append( XMLHandler.addTagValue( "memory_total", memoryTotal ) );

    xml.append( XMLHandler.addTagValue( "cpu_cores", cpuCores ) );
    xml.append( XMLHandler.addTagValue( "cpu_process_time", cpuProcessTime ) );

    xml.append( XMLHandler.addTagValue( "uptime", uptime ) );
    xml.append( XMLHandler.addTagValue( "thread_count", threadCount ) );

    xml.append( XMLHandler.addTagValue( "load_avg", loadAvg ) );

    xml.append( XMLHandler.addTagValue( "os_name", osName ) );
    xml.append( XMLHandler.addTagValue( "os_version", osVersion ) );
    xml.append( XMLHandler.addTagValue( "os_arch", osArchitecture ) );

    xml.append( "  <pipeline_status_list>" ).append( Const.CR );
    for ( int i = 0; i < pipelineStatusList.size(); i++ ) {
      SlaveServerPipelineStatus pipelineStatus = pipelineStatusList.get( i );
      xml.append( "    " ).append( pipelineStatus.getXML() ).append( Const.CR );
    }
    xml.append( "  </pipeline_status_list>" ).append( Const.CR );

    xml.append( "  <job_status_list>" ).append( Const.CR );
    for ( int i = 0; i < jobStatusList.size(); i++ ) {
      SlaveServerJobStatus jobStatus = jobStatusList.get( i );
      xml.append( "    " ).append( jobStatus.getXML() ).append( Const.CR );
    }
    xml.append( "  </job_status_list>" ).append( Const.CR );

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  public SlaveServerStatus( Node statusNode ) throws HopException {
    this();
    statusDescription = XMLHandler.getTagValue( statusNode, "statusdesc" );

    memoryFree = Const.toLong( XMLHandler.getTagValue( statusNode, "memory_free" ), -1L );
    memoryTotal = Const.toLong( XMLHandler.getTagValue( statusNode, "memory_total" ), -1L );

    String cpuCoresStr = XMLHandler.getTagValue( statusNode, "cpu_cores" );
    cpuCores = Const.toInt( cpuCoresStr, -1 );
    String cpuProcessTimeStr = XMLHandler.getTagValue( statusNode, "cpu_process_time" );
    cpuProcessTime = Utils.isEmpty( cpuProcessTimeStr ) ? 0L : Long.valueOf( cpuProcessTimeStr );

    uptime = Const.toLong( XMLHandler.getTagValue( statusNode, "uptime" ), -1 );
    threadCount = Const.toInt( XMLHandler.getTagValue( statusNode, "thread_count" ), -1 );
    loadAvg = Const.toDouble( XMLHandler.getTagValue( statusNode, "load_avg" ), -1.0 );

    osName = XMLHandler.getTagValue( statusNode, "os_name" );
    osVersion = XMLHandler.getTagValue( statusNode, "os_version" );
    osArchitecture = XMLHandler.getTagValue( statusNode, "os_arch" );

    Node listPipelineNode = XMLHandler.getSubNode( statusNode, "pipeline_status_list" );
    Node listJobsNode = XMLHandler.getSubNode( statusNode, "job_status_list" );

    int nrPipelines = XMLHandler.countNodes( listPipelineNode, SlaveServerPipelineStatus.XML_TAG );
    int nrJobs = XMLHandler.countNodes( listJobsNode, SlaveServerJobStatus.XML_TAG );

    for ( int i = 0; i < nrPipelines; i++ ) {
      Node pipelineStatusNode = XMLHandler.getSubNodeByNr( listPipelineNode, SlaveServerPipelineStatus.XML_TAG, i );
      pipelineStatusList.add( new SlaveServerPipelineStatus( pipelineStatusNode ) );
    }

    for ( int i = 0; i < nrJobs; i++ ) {
      Node jobStatusNode = XMLHandler.getSubNodeByNr( listJobsNode, SlaveServerJobStatus.XML_TAG, i );
      jobStatusList.add( new SlaveServerJobStatus( jobStatusNode ) );
    }
  }

  public static SlaveServerStatus fromXML( String xml ) throws HopException {
    Document document = XMLHandler.loadXMLString( xml );
    return new SlaveServerStatus( XMLHandler.getSubNode( document, XML_TAG ) );
  }

  /**
   * @return the statusDescription
   */
  public String getStatusDescription() {
    return statusDescription;
  }

  /**
   * @param statusDescription the statusDescription to set
   */
  public void setStatusDescription( String statusDescription ) {
    this.statusDescription = statusDescription;
  }

  /**
   * @return the pipelineStatusList
   */
  public List<SlaveServerPipelineStatus> getPipelineStatusList() {
    return pipelineStatusList;
  }

  /**
   * @param pipelineStatusList the pipelineStatusList to set
   */
  public void setPipelineStatusList( List<SlaveServerPipelineStatus> pipelineStatusList ) {
    this.pipelineStatusList = pipelineStatusList;
  }

  /**
   * @return the errorDescription
   */
  public String getErrorDescription() {
    return errorDescription;
  }

  /**
   * @param errorDescription the errorDescription to set
   */
  public void setErrorDescription( String errorDescription ) {
    this.errorDescription = errorDescription;
  }

  public SlaveServerPipelineStatus findPipelineStatus( String pipelineName, String id ) {
    for ( int i = 0; i < pipelineStatusList.size(); i++ ) {
      SlaveServerPipelineStatus pipelineStatus = pipelineStatusList.get( i );
      if ( pipelineStatus.getPipelineName().equalsIgnoreCase( pipelineName )
        && ( Utils.isEmpty( id ) || pipelineStatus.getId().equals( id ) ) ) {
        return pipelineStatus;
      }
    }
    return null;
  }

  public SlaveServerJobStatus findJobStatus( String jobName, String id ) {
    for ( int i = 0; i < jobStatusList.size(); i++ ) {
      SlaveServerJobStatus jobStatus = jobStatusList.get( i );
      if ( jobStatus.getJobName().equalsIgnoreCase( jobName )
        && ( Utils.isEmpty( id ) || jobStatus.getId().equals( id ) ) ) {
        return jobStatus;
      }
    }
    return null;
  }

  /**
   * @return the jobStatusList
   */
  public List<SlaveServerJobStatus> getJobStatusList() {
    return jobStatusList;
  }

  /**
   * @param jobStatusList the jobStatusList to set
   */
  public void setJobStatusList( List<SlaveServerJobStatus> jobStatusList ) {
    this.jobStatusList = jobStatusList;
  }

  /**
   * @return the memoryFree
   */
  public double getMemoryFree() {
    return memoryFree;
  }

  /**
   * @param memoryFree the memoryFree to set
   */
  public void setMemoryFree( long memoryFree ) {
    this.memoryFree = memoryFree;
  }

  /**
   * @return the memoryTotal
   */
  public double getMemoryTotal() {
    return memoryTotal;
  }

  /**
   * @param memoryTotal the memoryTotal to set
   */
  public void setMemoryTotal( long memoryTotal ) {
    this.memoryTotal = memoryTotal;
  }

  /**
   * @return the cpuCores
   */
  public int getCpuCores() {
    return cpuCores;
  }

  /**
   * @param cpuCores the cpuCores to set
   */
  public void setCpuCores( int cpuCores ) {
    this.cpuCores = cpuCores;
  }

  /**
   * @return the cpuProcessTime
   */
  public long getCpuProcessTime() {
    return cpuProcessTime;
  }

  /**
   * @param cpuProcessTime the cpuProcessTime to set
   */
  public void setCpuProcessTime( long cpuProcessTime ) {
    this.cpuProcessTime = cpuProcessTime;
  }

  public void setUptime( long uptime ) {
    this.uptime = uptime;
  }

  public long getUptime() {
    return uptime;
  }

  public void setThreadCount( int threadCount ) {
    this.threadCount = threadCount;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public void setLoadAvg( double loadAvg ) {
    this.loadAvg = loadAvg;
  }

  public double getLoadAvg() {
    return loadAvg;
  }

  public void setOsName( String osName ) {
    this.osName = osName;
  }

  public String getOsName() {
    return osName;
  }

  public void setOsVersion( String osVersion ) {
    this.osVersion = osVersion;
  }

  public String getOsVersion() {
    return osVersion;
  }

  public void setOsArchitecture( String osArch ) {
    this.osArchitecture = osArch;
  }

  public String getOsArchitecture() {
    return osArchitecture;
  }
}
