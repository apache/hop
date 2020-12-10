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
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.pipeline.PipelineMeta;

@ExtensionPoint(
  extensionPointId = "HopGuiPipelineAfterClose",
  id = "HopGuiPipelineAfterClose",
  description = "Cleanup the active unit test for the closed pipeline"
)
public class HopGuiPipelineAfterClose implements IExtensionPoint<PipelineMeta> {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, PipelineMeta pipelineMeta ) throws HopException {
    TestingGuiPlugin.getInstance().getActiveTests().remove( pipelineMeta );
  }
}
