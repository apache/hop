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

package org.apache.hop.lineage.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.LineageTransformSchemaEmitter;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;

@ExtensionPoint(
    id = "LineageHubTransformFinishXp",
    extensionPointId = "TransformFinished",
    description =
        "Emits lineage RUN_LIFECYCLE FINISHED or FAILED when a transform has finished executing, "
            + "plus the relational table access the transform declares on its metadata")
public class LineageHubTransformFinishXp implements IExtensionPoint<TransformMetaDataCombi> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, TransformMetaDataCombi combi) throws HopException {
    boolean failed = LineageRunLifecycleEmitter.transformFailed(combi);
    RunLifecyclePhase phase = failed ? RunLifecyclePhase.FAILED : RunLifecyclePhase.FINISHED;
    LineageRunLifecycleEmitter.emitTransform(combi, phase, null);
    LineageTransformSchemaEmitter.emitTransformBoundarySchemas(combi);

    // Relational lineage is derived from what the transform declares on its metadata rather than
    // emitted by the transform itself, so every transform annotated @RelationalLineage is covered
    // without needing its own emission code. Done at finish, where the row shape is populated and
    // the outcome is known.
    if (combi != null && combi.transform != null) {
      LineageRelationalIoEmitter.emitDeclaredRelationalIo(combi.transform, !failed);
    }
  }
}
