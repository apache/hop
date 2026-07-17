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

package org.apache.hop.workflow;

import java.util.Comparator;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

/**
 * This class holds the result of a action after it was executed. Things we want to keep track of
 * are:
 *
 * <p>--> result of the execution (Result)
 *
 * <p>--> ...
 */
@Getter
@Setter
public class ActionResult implements Cloneable, Comparator<ActionResult>, Comparable<ActionResult> {
  public static final String XML_TAG = "action_result";

  private static final String TAG_ACTION_NAME = "action_name";
  private static final String TAG_COMMENT = "comment";
  private static final String TAG_REASON = "reason";
  private static final String TAG_LOG_DATE = "log_date";
  private static final String TAG_ACTION_FILENAME = "action_filename";
  private static final String TAG_LOG_CHANNEL_ID = "log_channel_id";
  private static final String TAG_CHECKPOINT = "checkpoint";
  private static final String TAG_BYTES_READ = "bytes_read";
  private static final String TAG_BYTES_WRITTEN = "bytes_written";

  private Result result;
  private String actionName;

  private String comment;
  private String reason;

  private Date logDate;
  private String actionFilename;
  private String logChannelId;

  private boolean checkpoint;

  /** Bytes read by this action (per-action, not cumulative). */
  private long bytesRead;

  /** Bytes written by this action (per-action, not cumulative). */
  private long bytesWritten;

  /** Creates a new empty action result... */
  public ActionResult() {
    logDate = new Date();
  }

  /** Creates a new action result... */
  public ActionResult(
      Result result,
      String logChannelId,
      String comment,
      String reason,
      String actionName,
      String actionFilename) {
    this(result, logChannelId, comment, reason, actionName, actionFilename, 0L, 0L);
  }

  /**
   * Creates a new action result with per-action bytes read/written.
   *
   * @param bytesRead bytes read by this action only
   * @param bytesWritten bytes written by this action only
   */
  public ActionResult(
      Result result,
      String logChannelId,
      String comment,
      String reason,
      String actionName,
      String actionFilename,
      long bytesRead,
      long bytesWritten) {
    this();
    if (result != null) {
      // lightClone doesn't bother cloning all the rows.
      this.result = result.lightClone();
      this.result.setLogText(null);
    } else {
      this.result = null;
    }
    this.logChannelId = logChannelId;
    this.comment = comment;
    this.reason = reason;
    this.actionName = actionName;
    this.actionFilename = actionFilename;
    this.bytesRead = bytesRead;
    this.bytesWritten = bytesWritten;
  }

  /**
   * Reads an action result back from the XML written by {@link #getXml()}.
   *
   * @param node the {@link #XML_TAG} node to read
   */
  public ActionResult(Node node) throws HopException {
    actionName = XmlHandler.getTagValue(node, TAG_ACTION_NAME);
    comment = XmlHandler.getTagValue(node, TAG_COMMENT);
    reason = XmlHandler.getTagValue(node, TAG_REASON);
    logDate = XmlHandler.stringToDate(XmlHandler.getTagValue(node, TAG_LOG_DATE));
    actionFilename = XmlHandler.getTagValue(node, TAG_ACTION_FILENAME);
    logChannelId = XmlHandler.getTagValue(node, TAG_LOG_CHANNEL_ID);
    checkpoint = "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, TAG_CHECKPOINT));
    bytesRead = Const.toLong(XmlHandler.getTagValue(node, TAG_BYTES_READ), 0L);
    bytesWritten = Const.toLong(XmlHandler.getTagValue(node, TAG_BYTES_WRITTEN), 0L);

    // A result is only present once the action finished: the "start of action" entries carry none.
    //
    Node resultNode = XmlHandler.getSubNode(node, Result.XML_TAG);
    if (resultNode != null) {
      result = new Result(resultNode);
    }
  }

  /**
   * Serializes this action result, including the fields the workflow metrics view displays. The
   * result itself is left out when the action has not finished yet.
   */
  public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append(XmlHandler.openTag(XML_TAG));
    xml.append(XmlHandler.addTagValue(TAG_ACTION_NAME, actionName));
    xml.append(XmlHandler.addTagValue(TAG_COMMENT, comment));
    xml.append(XmlHandler.addTagValue(TAG_REASON, reason));
    xml.append(XmlHandler.addTagValue(TAG_LOG_DATE, XmlHandler.date2string(logDate)));
    xml.append(XmlHandler.addTagValue(TAG_ACTION_FILENAME, actionFilename));
    xml.append(XmlHandler.addTagValue(TAG_LOG_CHANNEL_ID, logChannelId));
    xml.append(XmlHandler.addTagValue(TAG_CHECKPOINT, checkpoint));
    xml.append(XmlHandler.addTagValue(TAG_BYTES_READ, bytesRead));
    xml.append(XmlHandler.addTagValue(TAG_BYTES_WRITTEN, bytesWritten));
    if (result != null) {
      xml.append(result.getBasicXml());
    }
    xml.append(XmlHandler.closeTag(XML_TAG));
    return xml.toString();
  }

  @Override
  public Object clone() {
    try {
      ActionResult actionResult = (ActionResult) super.clone();

      if (getResult() != null) {
        actionResult.setResult(getResult().clone());
      }

      return actionResult;
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public int compare(ActionResult one, ActionResult two) {
    if (one == null && two != null) {
      return -1;
    }
    if (one != null && two == null) {
      return 1;
    }
    if (one == null && two == null) {
      return 0;
    }
    if (one.getActionName() == null && two.getActionName() != null) {
      return -1;
    }
    if (one.getActionName() != null && two.getActionName() == null) {
      return 1;
    }
    if (one.getActionName() == null && two.getActionName() == null) {
      return 0;
    }

    return one.getActionName().compareTo(two.getActionName());
  }

  @Override
  public int compareTo(ActionResult two) {
    return compare(this, two);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ActionResult that = (ActionResult) o;
    return checkpoint == that.checkpoint
        && bytesRead == that.bytesRead
        && bytesWritten == that.bytesWritten
        && java.util.Objects.equals(result, that.result)
        && java.util.Objects.equals(actionName, that.actionName)
        && java.util.Objects.equals(comment, that.comment)
        && java.util.Objects.equals(reason, that.reason)
        && java.util.Objects.equals(logDate, that.logDate)
        && java.util.Objects.equals(actionFilename, that.actionFilename)
        && java.util.Objects.equals(logChannelId, that.logChannelId);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        result,
        actionName,
        comment,
        reason,
        logDate,
        actionFilename,
        logChannelId,
        checkpoint,
        bytesRead,
        bytesWritten);
  }
}
