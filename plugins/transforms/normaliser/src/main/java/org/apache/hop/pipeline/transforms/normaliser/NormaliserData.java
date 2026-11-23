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

package org.apache.hop.pipeline.transforms.normaliser;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class NormaliserData extends BaseTransformData implements ITransformData {
  /** The types, in the order their rows are written. */
  String[] types;

  /** Per type, in the order of {@link #types}: where its input fields go. */
  Placement[][] placements;

  /** The input fields that are copied to every output row, in input order. */
  int[] copyFieldNrs;

  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;

  /**
   * Where one input field goes on the output row of its type.
   *
   * @param inputIndex the input field
   * @param outputIndex the normalised field it fills
   * @param source the input field's metadata
   * @param target the normalised field's metadata when the value has to be converted into it, null
   *     when it is copied as it is
   */
  record Placement(int inputIndex, int outputIndex, IValueMeta source, IValueMeta target) {}
}
