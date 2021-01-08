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

package org.apache.hop.ui.hopgui.file.pipeline.extension;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

public class HopGuiPipelinePreviewExtension {

  private Composite previewTab;
  private Control previewToolbar;
  private Composite preview;

  /**
   * @param previewTab
   * @param previewToolbar
   */
  public HopGuiPipelinePreviewExtension( Composite previewTab, Control previewToolbar, Composite preview ) {
    this.preview = preview;
    this.previewTab = previewTab;
    this.previewToolbar = previewToolbar;
  }

  public Composite getPreviewTab() {
    return previewTab;
  }

  public Control getPreviewToolbar() {
    return previewToolbar;
  }

  public Composite getPreview() {
    return preview;
  }
}
