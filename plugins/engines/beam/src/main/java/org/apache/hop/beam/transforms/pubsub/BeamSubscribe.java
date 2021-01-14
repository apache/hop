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

package org.apache.hop.beam.transforms.pubsub;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class BeamSubscribe extends BaseTransform<BeamSubscribeMeta, BeamSubscribeData> implements ITransform<BeamSubscribeMeta, BeamSubscribeData> {

  public BeamSubscribe( TransformMeta transformMeta, BeamSubscribeMeta meta, BeamSubscribeData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public boolean processRow() throws HopException {

    // Outside of a Beam Runner this transform doesn't actually do anything, it's just metadata
    // This transform gets converted into Beam API calls in a pipeline
    //
    throw new HopException("The Beam Subscribe transform can only run in a Beam pipeline");
  }
}
