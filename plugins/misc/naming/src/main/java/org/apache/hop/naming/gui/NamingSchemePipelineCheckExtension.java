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

package org.apache.hop.naming.gui;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.naming.engine.NamingSchemeCheckRemarks;
import org.apache.hop.pipeline.CheckTransformsExtension;
import org.apache.hop.pipeline.PipelineMeta;

/** Adds naming-scheme errors to pipeline Verify (same rules as {@code hop naming-check}). */
@ExtensionPoint(
    id = "NamingSchemeAfterCheckTransforms",
    description = "Add naming-scheme remarks after pipeline verify",
    extensionPointId = "AfterCheckTransforms")
public class NamingSchemePipelineCheckExtension
    implements IExtensionPoint<CheckTransformsExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, CheckTransformsExtension extension)
      throws HopException {
    if (extension == null || extension.getPipelineMeta() == null) {
      return;
    }
    PipelineMeta pipeline = extension.getPipelineMeta();
    String location = Const.NVL(pipeline.getFilename(), Const.NVL(pipeline.getName(), "pipeline"));
    NamingSchemeCheckRemarks.addRemarks(
        pipeline, location, extension.getRemarks(), extension.getMetadataProvider());
  }
}
