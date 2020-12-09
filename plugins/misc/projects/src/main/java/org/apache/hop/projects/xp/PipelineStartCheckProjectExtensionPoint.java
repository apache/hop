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

package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;

@ExtensionPoint(
  id = "PipelineStartCheckProjectExtensionPoint",
  description = "At the start of a pipeline, verify it lives in the active project",
  extensionPointId = "PipelinePrepareExecution"
)
/**
 * validate whether or not the pipeline about to be executed is part of the current project
 */
public class PipelineStartCheckProjectExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    String filename = pipeline.getFilename();

    try {
      ProjectsUtil.validateFileInProject( log, filename, pipeline );
    } catch ( Exception e ) {
      throw new HopException( "Validation error against pipeline '" + filename + "' in active project", e );
    }
  }

}
