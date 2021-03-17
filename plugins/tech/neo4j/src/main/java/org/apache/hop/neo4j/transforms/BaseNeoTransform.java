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
 *
 */

package org.apache.hop.neo4j.transforms;

import org.apache.hop.neo4j.core.Neo4jDefaults;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.HashMap;

public abstract class BaseNeoTransform<
        Meta extends ITransformMeta, Data extends BaseNeoTransformData>
    extends BaseTransform<Meta, Data> {

  public BaseNeoTransform(
      TransformMeta transformMeta,
      Meta meta,
      Data data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    data.usageMap = new HashMap<>();

    return super.init();
  }

  @Override
  public void dispose() {

    getPipeline().getExtensionDataMap().put(Neo4jDefaults.TRANS_NODE_UPDATES_GROUP, data.usageMap);

    super.dispose();
  }
}
