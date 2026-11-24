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

package org.apache.hop.beam.transform;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.pipeline.transform.BaseTransformMeta;

/**
 * Test transform with the plugin id of Blocking transform, one of the transforms Beam runs on all
 * rows at once: it writes only the last row of its input, once the input ends.
 */
@Transform(
    id = LastRowOnlyTransformMeta.ID,
    name = "Last row only test transform",
    description = "Writes the last row once the input ends",
    categoryDescription = "Testing")
public class LastRowOnlyTransformMeta
    extends BaseTransformMeta<LastRowOnlyTransform, LastRowOnlyTransformData> {
  public static final String ID = "BlockingTransform";
}
