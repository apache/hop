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

package org.apache.hop.pipeline.transforms.odata;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "ODataInput",
    image = "odata.svg",
    name = "i18n::ODataInput.Name",
    description = "i18n::ODataInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::ODataInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/odata-input.html")
public class ODataInputMeta extends BaseTransformMeta<ODataInput, ODataInputData> {
  private static final Class<?> PKG = ODataInputMeta.class;

  @HopMetadataProperty(key = "url")
  private String url;

  @HopMetadataProperty(key = "entity_set")
  private String entitySet;

  @HopMetadataProperty(key = "auth_type")
  private String authType;

  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  @HopMetadataProperty(key = "token", password = true)
  private String token;

  @HopMetadataProperty(key = "query_select")
  private String querySelect;

  @HopMetadataProperty(key = "query_filter")
  private String queryFilter;

  @HopMetadataProperty(key = "query_order")
  private String queryOrder;

  @HopMetadataProperty(key = "query_top")
  private String queryTop;

  @HopMetadataProperty(key = "query_skip")
  private String querySkip;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<ODataField> fields;

  public ODataInputMeta() {
    super();
    fields = new ArrayList<>();
    authType = "NONE";
  }

  public ODataInputMeta(ODataInputMeta other) {
    this();
    this.url = other.url;
    this.entitySet = other.entitySet;
    this.authType = other.authType;
    this.username = other.username;
    this.password = other.password;
    this.token = other.token;
    this.querySelect = other.querySelect;
    this.queryFilter = other.queryFilter;
    this.queryOrder = other.queryOrder;
    this.queryTop = other.queryTop;
    this.querySkip = other.querySkip;
    if (other.fields != null) {
      for (ODataField f : other.fields) {
        this.fields.add(new ODataField(f));
      }
    }
  }

  @Override
  public void setDefault() {
    this.url = "";
    this.entitySet = "";
    this.authType = "NONE";
    this.username = "";
    this.password = "";
    this.token = "";
    this.querySelect = "";
    this.queryFilter = "";
    this.queryOrder = "";
    this.queryTop = "";
    this.querySkip = "";
    this.fields = new ArrayList<>();
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (ODataField field : fields) {
      try {
        rowMeta.addValueMeta(field.toValueMeta(name, variables));
      } catch (Exception e) {
        throw new HopTransformException(
            "Error generating value meta for field: " + field.getName(), e);
      }
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (url == null || url.trim().isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR, "Service URL is missing.", transformMeta));
    } else {
      remarks.add(
          new CheckResult(ICheckResult.TYPE_RESULT_OK, "Service URL is specified.", transformMeta));
    }

    if (entitySet == null || entitySet.trim().isEmpty()) {
      remarks.add(
          new CheckResult(ICheckResult.TYPE_RESULT_ERROR, "Entity Set is missing.", transformMeta));
    } else {
      remarks.add(
          new CheckResult(ICheckResult.TYPE_RESULT_OK, "Entity Set is specified.", transformMeta));
    }
  }
}
