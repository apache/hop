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
package org.apache.hop.lineage;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.DatasetIoLineagePayload;
import org.apache.hop.lineage.model.DatasetIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Emits {@link LineageEventKind#DATASET_IO}: a transform read or wrote a dataset that is neither a
 * table nor a file, such as a Kafka topic.
 */
public final class LineageDatasetIoEmitter {

  private LineageDatasetIoEmitter() {}

  /**
   * Records that a transform reads or writes a dataset. Does nothing without a namespace and a
   * name, or when lineage is off for the transform's run. Never throws: lineage must not change
   * what the transform does.
   *
   * @param transform the transform (must not be null)
   * @param operation whether it reads or writes the dataset
   * @param namespace the platform and where it runs, such as {@code kafka://broker:9092}
   * @param name the dataset within the namespace, such as a topic
   * @param success whether the read or write went through
   * @param message optional short detail (failure reason, etc.)
   */
  public static void emitTransformDatasetIo(
      ITransform transform,
      DatasetIoOperation operation,
      String namespace,
      String name,
      boolean success,
      String message) {
    if (transform == null
        || operation == null
        || Utils.isEmpty(namespace)
        || Utils.isEmpty(name)
        || !LineageSinkRoutes.isEnabled(transform)) {
      return;
    }
    try {
      LineageContext.Builder ctx = LineageRunLifecycleEmitter.transformContextBuilder(transform);
      if (ctx == null) {
        return;
      }
      if (!Utils.isEmpty(transform.getTransformPluginId())) {
        ctx.putAttribute("transformPluginId", transform.getTransformPluginId());
      }
      LineageHub.getInstance()
          .emit(
              LineageEvent.of(
                  LineageEventKind.DATASET_IO,
                  ctx.build(),
                  new DatasetIoLineagePayload(operation, namespace, name, success, message)));
    } catch (Exception e) {
      ILogChannel log = transform.getLogChannel();
      if (log != null) {
        log.logDebug("Lineage skipped: " + e.getMessage());
      }
    }
  }
}
