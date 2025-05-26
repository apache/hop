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

import lombok.Getter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

public enum Status implements IEnumHasCodeAndDescription {

  /** The status empty. */
  EMPTY("Action.status.Empty"),
  /** The status running. */
  RUNNING("Action.status.Running"),
  /** The status finished. */
  FINISHED("Action.status.Finished"),
  /** The status stopped. */
  STOPPED("Action.status.Stopped"),
  /** The status paused. */
  PAUSED("Action.status.Paused");

  /** The status description. */
  @Getter private final String description;

  private Status(String description) {
    this.description = BaseMessages.getString(Status.class, description);
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String toString() {
    return description;
  }

  /**
   * Return the status for a certain code
   *
   * @param code the code to look for
   * @return the status or EMPTY if nothing matches.
   */
  public static Status lookupCode(String code) {
    return IEnumHasCode.lookupCode(Status.class, code, EMPTY);
  }

  /**
   * Return the status for a certain description
   *
   * @param description the description to look for
   * @return the status or EMPTY if nothing matches.
   */
  public static Status lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(Status.class, description, EMPTY);
  }
}
