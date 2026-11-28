/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformInitThread;
import org.apache.hop.spark.util.SparkConst;

/**
 * Marks the transforms of a mini-pipeline that runs a Hop transform on a Spark partition as running
 * in a parallel bundle, numbered after the partition, before they initialize.
 *
 * <p>Writers such as Text file output add {@code _<transform id>_<bundle>} to their file name in
 * that case, the way they do on the Beam engines, so the partitions do not write one file. They
 * open the file in {@code init()}, so marking them afterwards (as the engine did) was too late:
 * every partition wrote the same file. {@code Internal.Transform.CopyNr} stays the transform's own
 * copy number, 0: {@code BaseTransform.init()} sets it. Only pipelines whose variables carry {@link
 * SparkConst#VAR_PARTITION_NR} are touched: the local engine never sets it.
 */
@ExtensionPoint(
    id = "SparkPartitionBundleExtensionPoint",
    extensionPointId = "TransformBeforeInitialize",
    description = "Native Spark: a transform on a Spark partition writes its own files")
public class SparkPartitionBundleExtensionPoint implements IExtensionPoint<TransformInitThread> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables pipeline, TransformInitThread thread) {
    if (pipeline == null || thread == null || thread.getCombi() == null) {
      return;
    }
    String partition = pipeline.getVariable(SparkConst.VAR_PARTITION_NR);
    ITransformData data = thread.getCombi().data;
    if (StringUtils.isEmpty(partition) || data == null) {
      return;
    }
    data.setBeamContext(true);
    data.setBeamBundleNr(Integer.parseInt(partition));
  }
}
