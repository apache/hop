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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import org.apache.hop.core.injection.Injection;
import org.apache.hop.pipeline.transform.TransformMeta;

public class TargetTransformDefinition extends TransformDefinition {
  @Injection( name = "TARGET_TAG", group = "TARGET_TRANSFORMS" )
  public String tag = super.tag;
  @Injection( name = "TARGET_TRANSFORM_NAME", group = "TARGET_TRANSFORMS" )
  public String transformName = super.transformName;
  public TransformMeta transformMeta = super.transformMeta;
  @Injection( name = "TARGET_DESCRIPTION", group = "TARGET_TRANSFORMS" )
  public String description = super.description;

  public TargetTransformDefinition() {
    super();
  }

  public TargetTransformDefinition( String tag, String transformName, TransformMeta transformMeta, String description ) {
    super( tag, transformName, transformMeta, description );
  }

  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
