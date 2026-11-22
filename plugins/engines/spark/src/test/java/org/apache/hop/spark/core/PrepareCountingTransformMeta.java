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

package org.apache.hop.spark.core;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.BaseTransformMeta;

/** Test transform that needs a once-per-run preparation and counts it. */
@Transform(
    id = PrepareCountingTransformMeta.ID,
    name = "Prepare counting test transform",
    description = "Counts its once-per-run preparations",
    categoryDescription = "Testing")
public class PrepareCountingTransformMeta
    extends BaseTransformMeta<PrepareCountingTransform, PrepareCountingTransformData> {
  public static final String ID = "SparkPrepareCountingTestTransform";

  @Override
  public boolean needsPrepareOnce(IVariables variables) {
    return true;
  }
}
