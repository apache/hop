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

package org.apache.hop.www;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

public class HopServerStatus {
  public static final String XML_TAG = "serverstatus";

  private String statusDescription;
  private String errorDescription;

  private List<HopServerPipelineStatus> pipelineStatusList;
  private List<HopServerWorkflowStatus> jobStatusList;

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

  public HopServerStatus() {
    pipelineStatusList = new ArrayList<>();
    jobStatusList = new ArrayList<>();
  }

  public HopServerStatus( String statusDescription ) {
    this();
    this.statusDescription = statusDescription;
  }

  /**
   * @param statusDescription
   * @param pipelineStatusList
   * @param jobStatusList
   */
  public HopServerStatus( String statusDescription, List<HopServerPipelineStatus> pipelineStatusList,
                            List<HopServerWorkflowStatus> jobStatusList ) {
    this.statusDescription = statusDescription;
    this.pipelineStatusList = pipelineStatusList;
    this.jobStatusList = jobStatusList;
  }

  public String getXml() throws HopException {
    StringBuilder xml = new StringBuilder();

    xml.append( "<" + XML_TAG + ">" ).append( Const.CR );
    xml.append( XmlHandler.addTagValue( "statusdesc", statusDescription ) );

    xml.append( XmlHandler.addTagValue( "memory_free", memoryFree ) );
    xml.append( XmlHandler.addTagValue( "memory_total", memoryTotal ) );

    xml.append( XmlHandler.addTagValue( "cpu_cores", cpuCores ) );
    xml.append( XmlHandler.addTagValue( "cpu_process_time", cpuProcessTime ) );

    xml.append( XmlHandler.addTagValue( "uptime", uptime ) );
    xml.append( XmlHandler.addTagValue( "thread_count", threadCount ) );

    xml.append( XmlHandler.addTagValue( "load_avg", loadAvg ) );

    xml.append( XmlHandler.addTagValue( "os_name", osName ) );
    xml.append( XmlHandler.addTagValue( "os_version", osVersion ) );
    xml.append( XmlHandler.addTagValue( "os_arch", osArchitecture ) );

    xml.append( "  <pipeline_status_list>" ).append( Const.CR );
    for ( int i = 0; i < pipelineStatusList.size(); i++ ) {
      HopServerPipelineStatus pipelineStatus = pipelineStatusList.get( i );
      xml.append( "    " ).append( pipelineStatus.getXml() ).append( Const.CR );
    }
    xml.append( "  </pipeline_status_list>" ).append( Const.CR );

    xml.append( "  <job_status_list>" ).append( Const.CR );
    for ( int i = 0; i < jobStatusList.size(); i++ ) {
      HopServerWorkflowStatus jobStatus = jobStatusList.get( i );
      xml.append( "    " ).append( jobStatus.getXml() ).append( Const.CR );
    }
    xml.append( "  </job_status_list>" ).append( Const.CR );

    xml.append( "</" + XML_TAG + ">" ).append( Const.CR );

    return xml.toString();
  }

  public HopServerStatus( Node statusNode ) throws HopException {
    this();
    statusDescription = XmlHandler.getTagValue( statusNode, "statusdesc" );

    memoryFree = Const.toLong( XmlHandler.getTagValue( statusNode, "memory_free" ), -1L );
    memoryTotal = Const.toLong( XmlHandler.getTagValue( statusNode, "memory_total" ), -1L );

    String cpuCoresStr = XmlHandler.getTagValue( statusNode, "cpu_cores" );
    cpuCores = Const.toInt( cpuCoresStr, -1 );
    String cpuProcessTimeStr = XmlHandler.getTagValue( statusNode, "cpu_process_time" );
    cpuProcessTime = Utils.isEmpty( cpuProcessTimeStr ) ? 0L : Long.valueOf( cpuProcessTimeStr );

    uptime = Const.toLong( XmlHandler.getTagValue( statusNode, "uptime" ), -1 );
    threadCount = Const.toInt( XmlHandler.getTagValue( statusNode, "thread_count" ), -1 );
    loadAvg = Const.toDouble( XmlHandler.getTagValue( statusNode, "load_avg" ), -1.0 );

    osName = XmlHandler.getTagValue( statusNode, "os_name" );
    osVersion = XmlHandler.getTagValue( statusNode, "os_version" );
    osArchitecture = XmlHandler.getTagValue( statusNode, "os_arch" );

    Node listPipelineNode = XmlHandler.getSubNode( statusNode, "pipeline_status_list" );
    Node listWorkflowsNode = XmlHandler.getSubNode( statusNode, "job_status_list" );

    int nrPipelines = XmlHandler.countNodes( listPipelineNode, HopServerPipelineStatus.XML_TAG );
    int nrWorkflows = XmlHandler.countNodes( listWorkflowsNode, HopServerWorkflowStatus.XML_TAG );

    for ( int i = 0; i < nrPipelines; i++ ) {
      Node pipelineStatusNode = XmlHandler.getSubNodeByNr( listPipelineNode, HopServerPipelineStatus.XML_TAG, i );
      pipelineStatusList.add( new HopServerPipelineStatus( pipelineStatusNode ) );
    }

    for ( int i = 0; i < nrWorkflows; i++ ) {
      Node jobStatusNode = XmlHandler.getSubNodeByNr( listWorkflowsNode, HopServerWorkflowStatus.XML_TAG, i );
      jobStatusList.add( new HopServerWorkflowStatus( jobStatusNode ) );
    }
  }

  public static HopServerStatus fromXml(String xml ) throws HopException {
    Document document = XmlHandler.loadXmlString( xml );
    return new HopServerStatus( XmlHandler.getSubNode( document, XML_TAG ) );
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
  public List<HopServerPipelineStatus> getPipelineStatusList() {
    return pipelineStatusList;
  }

  /**
   * @param pipelineStatusList the pipelineStatusList to set
   */
  public void setPipelineStatusList( List<HopServerPipelineStatus> pipelineStatusList ) {
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

  public HopServerPipelineStatus findPipelineStatus( String pipelineName, String id ) {
    for ( int i = 0; i < pipelineStatusList.size(); i++ ) {
      HopServerPipelineStatus pipelineStatus = pipelineStatusList.get( i );
      if ( pipelineStatus.getPipelineName().equalsIgnoreCase( pipelineName )
        && ( Utils.isEmpty( id ) || pipelineStatus.getId().equals( id ) ) ) {
        return pipelineStatus;
      }
    }
    return null;
  }

  public HopServerWorkflowStatus findJobStatus( String workflowName, String id ) {
    for ( int i = 0; i < jobStatusList.size(); i++ ) {
      HopServerWorkflowStatus jobStatus = jobStatusList.get( i );
      if ( jobStatus.getWorkflowName().equalsIgnoreCase( workflowName )
        && ( Utils.isEmpty( id ) || jobStatus.getId().equals( id ) ) ) {
        return jobStatus;
      }
    }
    return null;
  }

  /**
   * @return the jobStatusList
   */
  public List<HopServerWorkflowStatus> getJobStatusList() {
    return jobStatusList;
  }

  /**
   * @param jobStatusList the jobStatusList to set
   */
  public void setJobStatusList( List<HopServerWorkflowStatus> jobStatusList ) {
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
