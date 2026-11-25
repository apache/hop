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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Test transform: adds n * 2 as "doubled", and sends the rows with n a multiple of 5 to its error
 * hop.
 */
@Transform(
    id = ErrorRoutingTransformMeta.ID,
    name = "Error routing test transform",
    description = "Sends the multiples of five to its error hop",
    categoryDescription = "Testing")
public class ErrorRoutingTransformMeta
    extends BaseTransformMeta<ErrorRoutingTransform, ErrorRoutingTransformData> {
  public static final String ID = "BeamErrorRoutingTestTransform";

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ValueMetaInteger doubled = new ValueMetaInteger("doubled");
    doubled.setOrigin(name);
    inputRowMeta.addValueMeta(doubled);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
