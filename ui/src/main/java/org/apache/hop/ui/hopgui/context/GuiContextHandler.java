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

package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.action.GuiAction;

import java.util.List;

public class GuiContextHandler implements IGuiContextHandler {

  private String contextId;
  private List<GuiAction> supportedActions;

  public GuiContextHandler( String contextId, List<GuiAction> supportedActions ) {
    this.contextId = contextId;
    this.supportedActions = supportedActions;
  }

  /**
   * Gets contextId
   *
   * @return value of contextId
   */
  @Override public String getContextId() {
    return contextId;
  }

  /**
   * @param contextId The contextId to set
   */
  public void setContextId( String contextId ) {
    this.contextId = contextId;
  }

  /**
   * Gets supportedActions
   *
   * @return value of supportedActions
   */
  @Override public List<GuiAction> getSupportedActions() {
    return supportedActions;
  }

  /**
   * @param supportedActions The supportedActions to set
   */
  public void setSupportedActions( List<GuiAction> supportedActions ) {
    this.supportedActions = supportedActions;
  }
}
