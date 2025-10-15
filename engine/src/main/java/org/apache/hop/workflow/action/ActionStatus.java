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

package org.apache.hop.workflow.action;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

@XmlRootElement
public class ActionStatus {
  public static final String XML_TAG = "action_status";

  /** The name of the action */
  @Getter @Setter private String name;

  /** The status of the action */
  @Getter @Setter private Status status;

  /** The result for this action execution or null if not executed yet. */
  @Getter @Setter private Result result;

  public ActionStatus() {}

  @JsonIgnore
  public String getXml() throws HopException {
    try {
      StringBuilder xml = new StringBuilder();
      xml.append(XmlHandler.openTag(XML_TAG));
      xml.append(XmlHandler.addTagValue("name", name));
      xml.append(XmlHandler.addTagValue("status", status.getCode()));
      if (result != null) {
        xml.append(result.getBasicXml());
      }
      xml.append(XmlHandler.closeTag(XML_TAG));
      return xml.toString();
    } catch (Exception e) {
      throw new HopException("Unable to serialize action '" + name + "' status data to XML", e);
    }
  }

  public ActionStatus(Node node) throws HopException {
    name = XmlHandler.getTagValue(node, "name");
    status = Status.lookupCode(XmlHandler.getTagValue(node, "status"));
    Node resultNode = XmlHandler.getSubNode(node, "result");
    if (resultNode != null) {
      result = new Result(resultNode);
    }
  }

  public ActionStatus fromXml(String xml) throws HopException {
    Document document = XmlHandler.loadXmlString(xml);
    return new ActionStatus(XmlHandler.getSubNode(document, XML_TAG));
  }
}
