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

package org.apache.hop.pipeline.transforms.missing;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;

public class Missing extends DummyMeta {

  private String transformName;
  private String missingPluginId;

  public Missing( String transformName, String missingPluginId ) {
    this.transformName = transformName;
    this.missingPluginId = missingPluginId;
  }

  public String getTransformName() {
    return transformName;
  }

  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  public String getMissingPluginId() {
    return missingPluginId;
  }

  public void setMissingPluginId( String missingPluginId ) {
    this.missingPluginId = missingPluginId;
  }

  public ITransform createTransform( TransformMeta transformMeta, DummyData data, int cnr, PipelineMeta tr,
                                     Pipeline pipeline ) {
    return new MissingTransform( transformMeta, this, data, cnr, tr, pipeline );
  }
}
