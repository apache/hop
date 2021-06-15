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

import org.apache.hop.core.Result;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.Comparator;
import java.util.Date;

/**
 * This class holds the result of a action after it was executed. Things we want to keep track of are:
 * <p>
 * --> result of the execution (Result)
 * <p>
 * --> ...
 * <p>
 *
 * @author Matt
 * @since 16-mrt-2005
 */
public class ActionResult implements Cloneable, Comparator<ActionResult>, Comparable<ActionResult> {
  private Result result;
  private String actionName;

  private String comment;
  private String reason;

  private Date logDate;
  private String actionFilename;
  private String logChannelId;

  private boolean checkpoint;

  /**
   * Creates a new empty action result...
   */
  public ActionResult() {
    logDate = new Date();
  }

  /**
   * Creates a new action result...
   */
  public ActionResult( Result result, String logChannelId, String comment, String reason, String actionName,
                       String actionFilename ) {
    this();
    if ( result != null ) {
      // lightClone doesn't bother cloning all the rows.
      this.result = result.lightClone();
      this.result.setLogText( null );
      // this.result.setRows(null);
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

      if ( getResult() != null ) {
        actionResult.setResult( getResult().clone() );
      }

      return actionResult;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * @param result The result to set.
   */
  public void setResult( Result result ) {
    this.result = result;
  }

  /**
   * @return Returns the result.
   */
  public Result getResult() {
    return result;
  }

  /**
   * @return Returns the comment.
   */
  public String getComment() {
    return comment;
  }

  /**
   * @param comment The comment to set.
   */
  public void setComment( String comment ) {
    this.comment = comment;
  }

  /**
   * @return Returns the reason.
   */
  public String getReason() {
    return reason;
  }

  /**
   * @param reason The reason to set.
   */
  public void setReason( String reason ) {
    this.reason = reason;
  }

  /**
   * @return Returns the logDate.
   */
  public Date getLogDate() {
    return logDate;
  }

  /**
   * @param logDate The logDate to set.
   */
  public void setLogDate( Date logDate ) {
    this.logDate = logDate;
  }

  /**
   * @return the actionName
   */
  public String getActionName() {
    return actionName;
  }

  /**
   * @param actionName the actionName to set
   */
  public void setActionName( String actionName ) {
    this.actionName = actionName;
  }

  /**
   * @return the actionFilename
   */
  public String getActionFilename() {
    return actionFilename;
  }

  /**
   * @param actionFilename the actionFilename to set
   */
  public void setActionFilename( String actionFilename ) {
    this.actionFilename = actionFilename;
  }

  @Override
  public int compare( ActionResult one, ActionResult two ) {
    if ( one == null && two != null ) {
      return -1;
    }
    if ( one != null && two == null ) {
      return 1;
    }
    if ( one == null && two == null ) {
      return 0;
    }
    if ( one.getActionName() == null && two.getActionName() != null ) {
      return -1;
    }
    if ( one.getActionName() != null && two.getActionName() == null ) {
      return 1;
    }
    if ( one.getActionName() == null && two.getActionName() == null ) {
      return 0;
    }

    return one.getActionName().compareTo( two.getActionName() );
  }

  @Override
  public int compareTo( ActionResult two ) {
    return compare( this, two );
  }

  public String getLogChannelId() {
    return logChannelId;
  }

  /**
   * @return the checkpoint
   */
  public boolean isCheckpoint() {
    return checkpoint;
  }

  /**
   * @param checkpoint the checkpoint to set
   */
  public void setCheckpoint( boolean checkpoint ) {
    this.checkpoint = checkpoint;
  }
}
