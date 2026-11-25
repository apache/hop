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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageRemoteRuns;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class WebResult {
  private static final Class<?> PKG = WebResult.class;

  public static final String XML_TAG = "webresult";

  public static final String STRING_OK = "OK";
  public static final String STRING_ERROR = "ERROR";

  public static final WebResult OK = new WebResult(STRING_OK);

  private String result;
  private String message;
  private String id;

  /**
   * The lineage run id a Hop Server reports a registered run under, when it reports the client's
   * run ({@link LineageRemoteRuns}); null otherwise, and from a server that doesn't. A client that
   * doesn't know it ignores it.
   */
  @Getter
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String lineageRunId;

  /** The names of the client's lineage sinks the server reports the registered run to. */
  @Getter
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<String> lineageSinks = List.of();

  /** Whether the server reports the run to sinks configured with variables like the client's. */
  @Getter
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private boolean lineageVariableSinks;

  public WebResult(String result) {
    this(result, null, null);
  }

  public WebResult(String result, String message) {
    this(result, message, null);
  }

  public WebResult(String result, String message, String id) {
    this.result = result;
    this.message = message;
    this.id = id;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append("<" + XML_TAG + ">").append(Const.CR);

    xml.append("  ").append(XmlHandler.addTagValue("result", result));
    xml.append("  ").append(XmlHandler.addTagValue("message", message));
    xml.append("  ").append(XmlHandler.addTagValue("id", id));
    if (lineageRunId != null) {
      xml.append("  ").append(XmlHandler.addTagValue("lineage_run_id", lineageRunId));
      xml.append("  ").append(XmlHandler.openTag("lineage_sinks")).append(Const.CR);
      for (String sink : lineageSinks) {
        xml.append("    ").append(XmlHandler.addTagValue("lineage_sink", sink));
      }
      xml.append("  ").append(XmlHandler.closeTag("lineage_sinks")).append(Const.CR);
      xml.append("  ")
          .append(XmlHandler.addTagValue("lineage_variable_sinks", lineageVariableSinks));
    }

    xml.append("</" + XML_TAG + ">").append(Const.CR);

    return xml.toString();
  }

  @JsonIgnore
  public String getJson() {
    try {
      return HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "{\"result\":\"ERROR\",\"message\":\"Failed to serialize response to JSON\",\"id\":null}";
    }
  }

  @Override
  public String toString() {
    return getXml();
  }

  public WebResult(Node webResultNode) {
    result = XmlHandler.getTagValue(webResultNode, "result");
    message = XmlHandler.getTagValue(webResultNode, "message");
    id = XmlHandler.getTagValue(webResultNode, "id");
    lineageRunId = XmlHandler.getTagValue(webResultNode, "lineage_run_id");
    Node sinksNode = XmlHandler.getSubNode(webResultNode, "lineage_sinks");
    if (sinksNode != null) {
      List<String> sinks = new ArrayList<>();
      for (Node sinkNode : XmlHandler.getNodes(sinksNode, "lineage_sink")) {
        String sink = XmlHandler.getNodeValue(sinkNode);
        if (sink != null) {
          sinks.add(sink);
        }
      }
      lineageSinks = List.copyOf(sinks);
    }
    lineageVariableSinks =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(webResultNode, "lineage_variable_sinks"));
  }

  public String getResult() {
    return result;
  }

  public void setResult(String result) {
    this.result = result;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public static WebResult fromXmlString(String xml) throws HopXmlException {
    try {
      Document doc = XmlHandler.loadXmlString(xml);
      Node node = XmlHandler.getSubNode(doc, XML_TAG);

      return new WebResult(node);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "WebResult.Error.UnableCreateResult"), e);
    }
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Adds what the server reports of a client's run it registered, if it reports one.
   *
   * @param answer what the server reports, or null
   * @return this result
   */
  public WebResult withLineage(LineageRemoteRuns.Answer answer) {
    if (answer != null) {
      lineageRunId = answer.runId();
      lineageSinks = answer.sinks() == null ? List.of() : List.copyOf(answer.sinks());
      lineageVariableSinks = answer.variableSinks();
    }
    return this;
  }

  /**
   * @return what the server reports of the client's run it registered, or null when it doesn't
   */
  @JsonIgnore
  public LineageRemoteRuns.Answer getLineageAnswer() {
    return lineageRunId == null
        ? null
        : new LineageRemoteRuns.Answer(lineageRunId, lineageSinks, lineageVariableSinks);
  }
}
