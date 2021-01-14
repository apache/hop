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

package org.apache.hop.ui.core.widget.warning;

public class SimpleWarningMessage implements IWarningMessage {

  private String warningMessage;
  private boolean warning;

  /**
   * @param warning
   * @param warningMessage
   */
  public SimpleWarningMessage(boolean warning, String warningMessage) {
    this.warning = warning;
    this.warningMessage = warningMessage;
  }

  /** @return the warningMessage */
  public String getWarningMessage() {
    return warningMessage;
  }

  /** @param warningMessage the warningMessage to set */
  public void setWarningMessage(String warningMessage) {
    this.warningMessage = warningMessage;
  }

  /** @return the warning */
  public boolean isWarning() {
    return warning;
  }

  /** @param warning the warning to set */
  public void setWarning(boolean warning) {
    this.warning = warning;
  }
}
