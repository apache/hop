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

package org.apache.hop.pipeline.transforms.selectvalues;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class SelectOptions {

  public SelectOptions() {
    selectFields = new ArrayList<>();
    deleteName = new ArrayList<>();
    meta = new ArrayList<>();
  }

  @HopMetadataProperty(
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "SelectValues.Injection.FIELDS")
  private List<SelectField> selectFields;

  @HopMetadataProperty(
      key = "select_unspecified",
      injectionKey = "SELECT_UNSPECIFIED",
      injectionKeyDescription = "SelectValues.Injection.SELECT_UNSPECIFIED")
  private boolean selectingAndSortingUnspecifiedFields;

  // DE-SELECT mode
  /** Names of the fields to be removed! */
  @HopMetadataProperty(
      key = "remove",
      injectionGroupKey = "REMOVES",
      injectionGroupDescription = "SelectValues.Injection.REMOVES")
  private List<DeleteField> deleteName;

  // META-DATA mode
  @HopMetadataProperty(
      key = "meta",
      injectionGroupKey = "METAS",
      injectionGroupDescription = "SelectValues.Injection.METAS")
  private List<SelectMetadataChange> meta;
}
