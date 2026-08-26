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

package org.apache.hop.ui.hopgui.perspective.explorer.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ui.util.HelpOpenMode;

@Getter
@Setter
public class ExplorerPerspectiveConfig {

  public static final String HOP_CONFIG_EXPLORER_PERSPECTIVE_CONFIG_KEY = "explorer-perspective";

  private String lazyLoadingDepth;
  private String fileLoadingMaxSize;
  private Boolean fileExplorerVisibleByDefault;
  private HelpOpenMode helpOpenMode;
  private Boolean activeFileSelection;

  public ExplorerPerspectiveConfig() {
    this.lazyLoadingDepth = "0";
    this.fileLoadingMaxSize = "16";
    this.fileExplorerVisibleByDefault = true;
    this.helpOpenMode = HelpOpenMode.BROWSER;
    this.activeFileSelection = true;
  }

  public ExplorerPerspectiveConfig(ExplorerPerspectiveConfig config) {
    this();
    this.lazyLoadingDepth = config.lazyLoadingDepth;
    this.fileLoadingMaxSize = config.fileLoadingMaxSize;
    this.fileExplorerVisibleByDefault = config.fileExplorerVisibleByDefault;
    this.helpOpenMode = config.getHelpOpenMode();
    this.activeFileSelection = config.activeFileSelection;
  }

  public HelpOpenMode getHelpOpenMode() {
    return helpOpenMode != null ? helpOpenMode : HelpOpenMode.BROWSER;
  }

  /**
   * Legacy hop-config key {@code openingHelpFiles}. True used to mean "open help in Explorer tabs".
   *
   * @param openingHelpFiles previous boolean flag
   */
  @JsonSetter("openingHelpFiles")
  public void migrateOpeningHelpFiles(Boolean openingHelpFiles) {
    if (Boolean.TRUE.equals(openingHelpFiles) && this.helpOpenMode == HelpOpenMode.BROWSER) {
      this.helpOpenMode = HelpOpenMode.TAB;
    }
  }

  /**
   * @return true when help should open as an Explorer tab (legacy checkbox semantics)
   * @deprecated use {@link #getHelpOpenMode()}
   */
  @Deprecated(since = "2.20")
  @JsonIgnore
  public Boolean isOpeningHelpFiles() {
    return getHelpOpenMode() == HelpOpenMode.TAB;
  }
}
