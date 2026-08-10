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

package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.TestType;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.ui.hopgui.HopGui;

@ExtensionPoint(
    id = "HopGuiUnitTestCreateBeforeDialog",
    extensionPointId = "HopGuiMetadataObjectCreateBeforeDialog",
    description = "Changes the name of the default unit test and calculates a relative path")
public class HopGuiUnitTestCreateBeforeDialog extends HopGuiUnitTestChanged
    implements IExtensionPoint {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {

    // Ignore all other metadata object changes
    //
    if (!(object instanceof PipelineUnitTest test)) {
      return;
    }
    PipelineMeta pipelineMeta = TestingGuiPlugin.getActivePipelineMeta();
    if (pipelineMeta == null) {
      return;
    }

    test.setName(uniqueUnitTestName(pipelineMeta.getName()));
    test.setType(TestType.UNIT_TEST);
    test.setRelativeFilename(HopGui.getInstance().getVariables(), pipelineMeta.getFilename());
  }

  /**
   * Build a default unit-test name that does not collide with an existing unit test.
   *
   * <p>{@code <pipeline> UNIT}, then {@code <pipeline> UNIT 2}, {@code UNIT 3}, ...
   */
  static String uniqueUnitTestName(String pipelineName) throws HopException {
    String base = (Utils.isEmpty(pipelineName) ? "Pipeline" : pipelineName.trim()) + " UNIT";
    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null || hopGui.getMetadataProvider() == null) {
      return base;
    }

    IHopMetadataSerializer<PipelineUnitTest> serializer =
        hopGui.getMetadataProvider().getSerializer(PipelineUnitTest.class);
    if (!serializer.exists(base)) {
      return base;
    }
    int n = 2;
    while (serializer.exists(base + " " + n)) {
      n++;
    }
    return base + " " + n;
  }
}
