/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HopServerStatus {
  public static final String XML_TAG = "serverstatus";

  @Getter @Setter private String statusDescription;
  @Getter @Setter private String errorDescription;
  @Getter @Setter private List<HopServerPipelineStatus> pipelineStatusList;
  @Getter @Setter private List<HopServerWorkflowStatus> workflowStatusList;
  @Getter @Setter private long memoryFree;
  @Getter @Setter private long memoryTotal;
  @Getter @Setter private int cpuCores;
  @Getter @Setter private long cpuProcessTime;
  @Getter @Setter private long uptime;
  @Getter @Setter private int threadCount;
  @Getter @Setter private double loadAvg;
  @Getter @Setter private String osName;
  @Getter @Setter private String osVersion;
  @Getter @Setter private String osArchitecture;

  public HopServerStatus() {
    pipelineStatusList = new ArrayList<>();
    workflowStatusList = new ArrayList<>();
  }

  public HopServerStatus(String statusDescription) {
    this();
    this.statusDescription = statusDescription;
  }

  /**
   * @param statusDescription
   * @param pipelineStatusList
   * @param workflowStatusList
   */
  public HopServerStatus(
      String statusDescription,
      List<HopServerPipelineStatus> pipelineStatusList,
      List<HopServerWorkflowStatus> workflowStatusList) {
    this.statusDescription = statusDescription;
    this.pipelineStatusList = pipelineStatusList;
    this.workflowStatusList = workflowStatusList;
  }

  @JsonIgnore
  public String getXml() throws HopException {
    StringBuilder xml = new StringBuilder();

    xml.append("<" + XML_TAG + ">").append(Const.CR);
    xml.append(XmlHandler.addTagValue("statusdesc", statusDescription));

    xml.append(XmlHandler.addTagValue("memory_free", memoryFree));
    xml.append(XmlHandler.addTagValue("memory_total", memoryTotal));

    xml.append(XmlHandler.addTagValue("cpu_cores", cpuCores));
    xml.append(XmlHandler.addTagValue("cpu_process_time", cpuProcessTime));

    xml.append(XmlHandler.addTagValue("uptime", uptime));
    xml.append(XmlHandler.addTagValue("thread_count", threadCount));

    xml.append(XmlHandler.addTagValue("load_avg", loadAvg));

    xml.append(XmlHandler.addTagValue("os_name", osName));
    xml.append(XmlHandler.addTagValue("os_version", osVersion));
    xml.append(XmlHandler.addTagValue("os_arch", osArchitecture));

    xml.append("  <pipeline_status_list>").append(Const.CR);
    for (HopServerPipelineStatus pipelineStatus : pipelineStatusList) {
      xml.append("    ").append(pipelineStatus.getXml()).append(Const.CR);
    }
    xml.append("  </pipeline_status_list>").append(Const.CR);

    xml.append("  <workflow_status_list>").append(Const.CR);
    for (HopServerWorkflowStatus workflowStatus : workflowStatusList) {
      xml.append("    ").append(workflowStatus.getXml()).append(Const.CR);
    }
    xml.append("  </workflow_status_list>").append(Const.CR);

    xml.append("</" + XML_TAG + ">").append(Const.CR);

    return xml.toString();
  }

  public HopServerStatus(Node statusNode) throws HopException {
    this();
    statusDescription = XmlHandler.getTagValue(statusNode, "statusdesc");

    memoryFree = Const.toLong(XmlHandler.getTagValue(statusNode, "memory_free"), -1L);
    memoryTotal = Const.toLong(XmlHandler.getTagValue(statusNode, "memory_total"), -1L);

    String cpuCoresStr = XmlHandler.getTagValue(statusNode, "cpu_cores");
    cpuCores = Const.toInt(cpuCoresStr, -1);
    String cpuProcessTimeStr = XmlHandler.getTagValue(statusNode, "cpu_process_time");
    cpuProcessTime = Utils.isEmpty(cpuProcessTimeStr) ? 0L : Long.valueOf(cpuProcessTimeStr);

    uptime = Const.toLong(XmlHandler.getTagValue(statusNode, "uptime"), -1);
    threadCount = Const.toInt(XmlHandler.getTagValue(statusNode, "thread_count"), -1);
    loadAvg = Const.toDouble(XmlHandler.getTagValue(statusNode, "load_avg"), -1.0);

    osName = XmlHandler.getTagValue(statusNode, "os_name");
    osVersion = XmlHandler.getTagValue(statusNode, "os_version");
    osArchitecture = XmlHandler.getTagValue(statusNode, "os_arch");

    Node listPipelineNode = XmlHandler.getSubNode(statusNode, "pipeline_status_list");
    Node listWorkflowsNode = XmlHandler.getSubNode(statusNode, "workflow_status_list");

    int nrPipelines = XmlHandler.countNodes(listPipelineNode, HopServerPipelineStatus.XML_TAG);
    int nrWorkflows = XmlHandler.countNodes(listWorkflowsNode, HopServerWorkflowStatus.XML_TAG);

    for (int i = 0; i < nrPipelines; i++) {
      Node pipelineStatusNode =
          XmlHandler.getSubNodeByNr(listPipelineNode, HopServerPipelineStatus.XML_TAG, i);
      pipelineStatusList.add(new HopServerPipelineStatus(pipelineStatusNode));
    }

    for (int i = 0; i < nrWorkflows; i++) {
      Node jobStatusNode =
          XmlHandler.getSubNodeByNr(listWorkflowsNode, HopServerWorkflowStatus.XML_TAG, i);
      workflowStatusList.add(new HopServerWorkflowStatus(jobStatusNode));
    }
  }

  public static HopServerStatus fromXml(String xml) throws HopException {
    Document document = XmlHandler.loadXmlString(xml);
    return new HopServerStatus(XmlHandler.getSubNode(document, XML_TAG));
  }

  public HopServerPipelineStatus findPipelineStatus(String pipelineName, String id) {
    for (HopServerPipelineStatus pipelineStatus : pipelineStatusList) {
      if (pipelineStatus.getPipelineName().equalsIgnoreCase(pipelineName)
          && (Utils.isEmpty(id) || pipelineStatus.getId().equals(id))) {
        return pipelineStatus;
      }
    }
    return null;
  }

  public HopServerWorkflowStatus findWorkflowStatus(String workflowName, String id) {
    for (HopServerWorkflowStatus workflowStatus : workflowStatusList) {
      if (workflowStatus.getWorkflowName().equalsIgnoreCase(workflowName)
          && (Utils.isEmpty(id) || workflowStatus.getId().equals(id))) {
        return workflowStatus;
      }
    }
    return null;
  }
}
