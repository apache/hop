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

package org.apache.hop.pipeline;

import lombok.Getter;

/**
 * Payload for {@link org.apache.hop.core.extension.HopExtensionPoint#PipelineTransformRenamed}.
 * Fired when a transform dialog commits a new name, before the live {@link
 * org.apache.hop.pipeline.transform.TransformMeta} is updated.
 */
@Getter
public class TransformNameChange {
  private final PipelineMeta pipelineMeta;
  private final String oldName;
  private final String newName;

  public TransformNameChange(PipelineMeta pipelineMeta, String oldName, String newName) {
    this.pipelineMeta = pipelineMeta;
    this.oldName = oldName;
    this.newName = newName;
  }
}
