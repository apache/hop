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
import org.apache.hop.core.Result;

/**
 * This class holds the result of a action after it was executed. Things we want to keep track of
 * are:
 *
 * <p>--> result of the execution (Result)
 *
 * <p>--> ...
 *
 * <p>
 */
@Getter
@Setter
public class ActionResult implements Cloneable, Comparator<ActionResult>, Comparable<ActionResult> {
  private Result result;
  private String actionName;

  private String comment;
  private String reason;

  private Date logDate;
  private String actionFilename;
  private String logChannelId;

  private boolean checkpoint;

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
        result, actionName, comment, reason, logDate, actionFilename, logChannelId, checkpoint);
  }
}
