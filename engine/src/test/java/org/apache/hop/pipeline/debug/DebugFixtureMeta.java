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

package org.apache.hop.pipeline.debug;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Tiny transform used to exercise the debug controller. Not a user-facing plugin. */
@Transform(id = "DebugFixture", name = "Debug fixture")
@Getter
@Setter
public class DebugFixtureMeta extends BaseTransformMeta<DebugFixture, DebugFixtureData> {

  public enum Mode {
    GENERATE,
    PASS,
    DROP_EVEN
  }

  private Mode mode = Mode.PASS;
  private int rowsToGenerate = 1;

  @Override
  public void setDefault() {
    mode = Mode.PASS;
    rowsToGenerate = 1;
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (mode == Mode.GENERATE && rowMeta.searchValueMeta("n") == null) {
      rowMeta.addValueMeta(new ValueMetaInteger("n"));
    }
  }
}
