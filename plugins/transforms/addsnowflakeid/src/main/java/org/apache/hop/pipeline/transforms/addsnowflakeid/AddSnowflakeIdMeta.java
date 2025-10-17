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

package org.apache.hop.pipeline.transforms.addsnowflakeid;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Meta data for the Add Snowflake Id transform
 *
 * @author lance
 * @since 2025/10/16 21:22
 */
@Transform(
    id = "SnowflakeId",
    image = "addsnowflakeid.svg",
    name = "i18n::BaseTransform.TypeLongDesc.AddSnowflakeId",
    description = "i18n::BaseTransform.TypeTooltipDesc.AddSnowflakeId",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/addsnowflakeid.html",
    keywords = "i18n::AddSnowflakeIdMeta.keyword")
@Getter
@Setter
public class AddSnowflakeIdMeta extends BaseTransformMeta<AddSnowflakeId, AddSnowflakeIdData> {
  private static final Class<?> PKG = AddSnowflakeIdMeta.class;

  @HopMetadataProperty(
      key = "val_name",
      injectionKeyDescription = "AddSnowflakeIdMeta.Injection.valueName")
  private String valueName;

  @HopMetadataProperty(
      key = "data_center_id",
      injectionKeyDescription = "AddSnowflakeIdMeta.Injection.DataCenterId")
  private Integer dataCenterId;

  @HopMetadataProperty(
      key = "machine_id",
      injectionKeyDescription = "AddSnowflakeIdMeta.Injection.MachineId")
  private Integer machineId;

  @Override
  public void setDefault() {
    valueName = "snowflakeId";
    dataCenterId = 1;
    machineId = 1;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider provider)
      throws HopTransformException {
    IValueMeta v = new ValueMetaInteger(valueName);
    v.setOrigin(name);
    row.addValueMeta(v);
  }
}
