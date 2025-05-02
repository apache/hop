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

package org.apache.hop.pipeline.transform;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.owasp.encoder.Encode;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

@XmlRootElement
public class TransformStatus {
  public static final String XML_TAG = "transform_status";
  private static final String CONST_SAMPLES = "samples";

  @Setter @Getter private String transformName;
  @Getter @Setter private int copy;
  @Setter @Getter private long linesRead;
  @Setter @Getter private long linesWritten;
  @Setter @Getter private long linesInput;
  @Getter @Setter private long linesOutput;
  @Setter @Getter private long linesUpdated;
  @Setter @Getter private long linesRejected;
  @Setter @Getter private long inputBufferSize;
  @Setter @Getter private long outputBufferSize;
  @Setter @Getter private long errors;
  @Setter @Getter private String statusDescription;
  @Setter @Getter private double seconds;
  @Setter @Getter private String speed;
  @Setter @Getter private String priority;
  @Setter @Getter private boolean stopped;
  @Setter @Getter private boolean paused;
  @Setter @Getter private String logText;
  @Setter @Getter private IRowMeta sampleRowMeta;
  @Setter @Getter private List<Object[]> sampleRows;

  private long accumulatedRuntime;

  private final DecimalFormat speedDf = new DecimalFormat("#,###,###,###,##0");

  public TransformStatus() {
    sampleRows = Collections.synchronizedList(new LinkedList<>());
  }

  public TransformStatus(IEngineComponent component) {
    updateAll(component);
  }

  public synchronized void updateAll(IEngineComponent component) {
    // Proc: nr of lines processed: input + output!

    this.transformName = component.getName();
    this.copy = component.getCopyNr();
    this.linesRead = linesRead + component.getLinesRead();
    this.linesWritten = linesWritten + component.getLinesWritten();
    this.linesInput = linesInput + component.getLinesInput();
    this.linesOutput = linesOutput + component.getLinesOutput();
    this.linesUpdated = linesUpdated + component.getLinesUpdated();
    this.linesRejected = linesRejected + component.getLinesRejected();
    this.errors = errors + component.getErrors();
    this.accumulatedRuntime = accumulatedRuntime + component.getExecutionDuration();
    this.statusDescription = component.getStatusDescription();
    this.logText = component.getLogText();

    long inProc = Math.max(linesInput, linesRead);
    long outProc = Math.max(linesOutput + linesUpdated, linesWritten + linesRejected);

    float lapsed = ((float) accumulatedRuntime) / 1000;
    double inSpeed = 0;
    double outSpeed = 0;

    if (lapsed != 0) {
      inSpeed = Math.floor(10 * (inProc / lapsed)) / 10;
      outSpeed = Math.floor(10 * (outProc / lapsed)) / 10;
    }

    double speedNumber = Math.max(inSpeed, outSpeed);

    this.seconds = Math.floor((lapsed * 10) + 0.5) / 10;
    this.speed = lapsed == 0 ? "-" : " " + speedDf.format(speedNumber);
    this.priority =
        component.isRunning()
            ? "   " + component.getInputBufferSize() + "/" + component.getOutputBufferSize()
            : "-";
    this.stopped = component.isStopped();
    this.paused = component.isPaused();

    // get the total input and output buffer size (if there are any)
    //
    this.inputBufferSize += component.getInputBufferSize();
    this.outputBufferSize += component.getOutputBufferSize();
  }

  public String getHTMLTableRow(boolean urlInTransformName) {
    return "<tr> "
        + "<th>"
        + (urlInTransformName ? transformName : Encode.forHtml(transformName))
        + "</th> "
        + "<th>"
        + copy
        + "</th> "
        + "<th>"
        + linesRead
        + "</th> "
        + "<th>"
        + linesWritten
        + "</th> "
        + "<th>"
        + linesInput
        + "</th> "
        + "<th>"
        + linesOutput
        + "</th> "
        + "<th>"
        + linesUpdated
        + "</th> "
        + "<th>"
        + linesRejected
        + "</th> "
        + "<th>"
        + errors
        + "</th> "
        + "<th>"
        + Encode.forHtml(statusDescription)
        + "</th> "
        + "<th>"
        + seconds
        + "</th> "
        + "<th>"
        + Encode.forHtml(speed)
        + "</th> "
        + "<th>"
        + Encode.forHtml(priority)
        + "</th> "
        + "</tr>";
  }

  @JsonIgnore
  public String getXml() throws HopException {
    try {
      StringBuilder xml = new StringBuilder();
      xml.append(XmlHandler.openTag(XML_TAG));

      xml.append(XmlHandler.addTagValue("transformName", transformName, false));
      xml.append(XmlHandler.addTagValue("copy", copy, false));
      xml.append(XmlHandler.addTagValue("linesRead", linesRead, false));
      xml.append(XmlHandler.addTagValue("linesWritten", linesWritten, false));
      xml.append(XmlHandler.addTagValue("linesInput", linesInput, false));
      xml.append(XmlHandler.addTagValue("linesOutput", linesOutput, false));
      xml.append(XmlHandler.addTagValue("linesUpdated", linesUpdated, false));
      xml.append(XmlHandler.addTagValue("linesRejected", linesRejected, false));
      xml.append(XmlHandler.addTagValue("errors", errors, false));
      xml.append(XmlHandler.addTagValue("input_buffer_size", inputBufferSize, false));
      xml.append(XmlHandler.addTagValue("output_buffer_size", outputBufferSize, false));
      xml.append(XmlHandler.addTagValue("statusDescription", statusDescription, false));
      xml.append(XmlHandler.addTagValue("seconds", seconds, false));
      xml.append(XmlHandler.addTagValue("speed", speed, false));
      xml.append(XmlHandler.addTagValue("priority", priority, false));
      xml.append(XmlHandler.addTagValue("stopped", stopped, false));
      xml.append(XmlHandler.addTagValue("paused", paused, false));
      xml.append(XmlHandler.addTagValue("log_text", XmlHandler.buildCDATA(logText)));

      if (sampleRowMeta != null) {
        xml.append(XmlHandler.openTag(CONST_SAMPLES));
        xml.append(sampleRowMeta.getMetaXml());
        xml.append(Const.CR);
        if (sampleRows != null) {
          synchronized (sampleRows) {
            Iterator<Object[]> iterator = sampleRows.iterator();
            while (iterator.hasNext()) {
              Object[] sampleRow = iterator.next();
              xml.append(sampleRowMeta.getDataXml(sampleRow));
              xml.append(Const.CR);
            }
          }
        }
        xml.append(XmlHandler.closeTag(CONST_SAMPLES));
      }

      xml.append(XmlHandler.closeTag(XML_TAG));
      return xml.toString();
    } catch (Exception e) {
      throw new HopException(
          "Unable to serialize transform '" + transformName + "' status data to XML", e);
    }
  }

  public TransformStatus(Node node) throws HopException {
    transformName = XmlHandler.getTagValue(node, "transformName");
    copy = Integer.parseInt(XmlHandler.getTagValue(node, "copy"));
    linesRead = Long.parseLong(XmlHandler.getTagValue(node, "linesRead"));
    linesWritten = Long.parseLong(XmlHandler.getTagValue(node, "linesWritten"));
    linesInput = Long.parseLong(XmlHandler.getTagValue(node, "linesInput"));
    linesOutput = Long.parseLong(XmlHandler.getTagValue(node, "linesOutput"));
    linesUpdated = Long.parseLong(XmlHandler.getTagValue(node, "linesUpdated"));
    linesRejected = Long.parseLong(XmlHandler.getTagValue(node, "linesRejected"));
    errors = Long.parseLong(XmlHandler.getTagValue(node, "errors"));
    inputBufferSize = Long.parseLong(XmlHandler.getTagValue(node, "input_buffer_size"));
    outputBufferSize = Long.parseLong(XmlHandler.getTagValue(node, "output_buffer_size"));
    statusDescription = XmlHandler.getTagValue(node, "statusDescription");
    seconds = Double.parseDouble(XmlHandler.getTagValue(node, "seconds"));
    speed = XmlHandler.getTagValue(node, "speed");
    priority = XmlHandler.getTagValue(node, "priority");
    stopped = "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "stopped"));
    paused = "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "paused"));

    String logTextData = XmlHandler.getTagValue(node, "log_text");
    if (!Utils.isEmpty(logTextData)) {
      logText = logTextData.substring("<![CDATA[".length(), logTextData.length() - "]]>".length());
    }

    Node samplesNode = XmlHandler.getSubNode(node, CONST_SAMPLES);
    if (samplesNode != null) {
      Node rowMetaNode = XmlHandler.getSubNode(samplesNode, RowMeta.XML_META_TAG);
      if (rowMetaNode != null) {
        sampleRowMeta = new RowMeta(rowMetaNode);
        sampleRows = new ArrayList<>();
        List<Node> dataNodes = XmlHandler.getNodes(samplesNode, RowMeta.XML_DATA_TAG);
        for (Node dataNode : dataNodes) {
          Object[] sampleRow = sampleRowMeta.getRow(dataNode);
          sampleRows.add(sampleRow);
        }
      }
    }
  }

  public TransformStatus fromXml(String xml) throws HopException {
    Document document = XmlHandler.loadXmlString(xml);
    return new TransformStatus(XmlHandler.getSubNode(document, XML_TAG));
  }

  @JsonIgnore
  public String[] getPipelineLogFields() {
    return getPipelineLogFields(statusDescription);
  }

  public String[] getPipelineLogFields(String overrideDescription) {
    return new String[] {
      "", // Row number
      transformName,
      Integer.toString(copy),
      Long.toString(linesRead),
      Long.toString(linesWritten),
      Long.toString(linesInput),
      Long.toString(linesOutput),
      Long.toString(linesUpdated),
      Long.toString(linesRejected),
      Long.toString(errors),
      overrideDescription,
      convertSeconds(seconds),
      speed,
      priority,
    };
  }

  private String convertSeconds(double seconds) {
    String retval = seconds + "s";

    if (seconds < 60) {
      return retval;
    }

    int mn = (int) seconds / 60;
    int h = mn / 60;
    mn = mn % 60;
    int s = (int) seconds % 60;

    if (h > 0) {
      retval = h + "h " + mn + "mn " + s + "s";
    } else {
      if (mn > 0) {
        retval = mn + "mn " + s + "s";
      } else {
        retval = seconds + "s";
      }
    }

    return retval;
  }

  @JsonIgnore
  public String[] getPeekFields() {
    return new String[] {
      Integer.toString(copy),
      Long.toString(linesRead),
      Long.toString(linesWritten),
      Long.toString(linesInput),
      Long.toString(linesOutput),
      Long.toString(linesUpdated),
      Long.toString(linesRejected),
      Long.toString(errors),
      statusDescription,
      convertSeconds(seconds),
      speed,
      priority,
    };
  }
}
