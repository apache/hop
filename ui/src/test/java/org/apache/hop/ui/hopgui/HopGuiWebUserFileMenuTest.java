/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Const;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;

class HopGuiWebUserFileMenuTest {

  @Test
  void browserSaveAllowsUnchangedFlowsButRejectsOtherFileTypesAndDeniedPermission() {
    IHopFileTypeHandler handler = mock(IHopFileTypeHandler.class);
    when(handler.hasChanged()).thenReturn(false);
    when(handler.getSubject()).thenReturn(new PipelineMeta());
    assertTrue(HopWebUserFileMenuState.canDownload(handler, true));
    when(handler.getSubject()).thenReturn(new WorkflowMeta());
    assertTrue(HopWebUserFileMenuState.canDownload(handler, true));
    assertFalse(HopWebUserFileMenuState.canDownload(handler, false));
    when(handler.getSubject()).thenReturn("text editor");
    assertFalse(HopWebUserFileMenuState.canDownload(handler, true));
    assertFalse(HopWebUserFileMenuState.canDownload(null, true));
  }

  @Test
  void projectExportRequiresAnActiveProjectPluginAndPermission() {
    assertFalse(HopWebUserFileMenuState.shouldShowProjectExport(null, true, true));
    assertFalse(HopWebUserFileMenuState.shouldShowProjectExport("", true, true));
    assertFalse(
        HopWebUserFileMenuState.shouldShowProjectExport(Const.VAR_PROJECT_HOME, true, true));
    assertFalse(HopWebUserFileMenuState.shouldShowProjectExport("project", false, true));
    assertFalse(HopWebUserFileMenuState.shouldShowProjectExport("project", true, false));
    assertTrue(HopWebUserFileMenuState.shouldShowProjectExport("project", true, true));
  }

  @Test
  void kettleImportRequiresThePluginAndWritePermissions() {
    assertFalse(HopWebUserFileMenuState.shouldShowKettleImport(false, true, true));
    assertFalse(HopWebUserFileMenuState.shouldShowKettleImport(true, false, true));
    assertFalse(HopWebUserFileMenuState.shouldShowKettleImport(true, true, false));
    assertTrue(HopWebUserFileMenuState.shouldShowKettleImport(true, true, true));
  }

  @Test
  void svgExportRequiresAnOpenFlowAndExportPermission() {
    assertFalse(HopWebUserFileMenuState.shouldShowSvgExport(false, false, true));
    assertFalse(HopWebUserFileMenuState.shouldShowSvgExport(true, false, false));
    assertFalse(HopWebUserFileMenuState.shouldShowSvgExport(false, true, false));
    assertTrue(HopWebUserFileMenuState.shouldShowSvgExport(true, false, true));
    assertTrue(HopWebUserFileMenuState.shouldShowSvgExport(false, true, true));
  }
}
