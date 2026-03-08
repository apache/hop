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
 *
 */

package org.apache.hop.pipeline.transforms.splunkinput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SplunkInput",
    name = "Splunk Input",
    description = "Read data from Splunk",
    image = "splunk.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::SplunkInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/splunkinput.html")
@Getter
@Setter
public class SplunkInputMeta extends BaseTransformMeta<SplunkInput, SplunkInputData> {
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "connection",
      injectionKeyDescription = "SplunkInputMeta.Injection.Connection")
  private String connectionName;

  @HopMetadataProperty(
      key = "query",
      injectionKey = "query",
      injectionKeyDescription = "SplunkInputMeta.Injection.Query")
  private String query;

  @HopMetadataProperty(
      groupKey = "returns",
      injectionGroupKey = "returns",
      injectionKeyDescription = "SplunkInputMeta.Injection.Returns",
      key = "return",
      injectionKey = "return",
      injectionGroupDescription = "SplunkInputMeta.Injection.Return")
  private List<ReturnValue> returnValues;

  public SplunkInputMeta() {
    super();
    returnValues = new ArrayList<>();
    this.query = "search * | head 100";
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

    for (ReturnValue returnValue : returnValues) {
      try {
        int type = ValueMetaFactory.getIdForValueMeta(returnValue.getType());
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(returnValue.getName(), type);
        valueMeta.setLength(returnValue.getLength());
        valueMeta.setOrigin(name);
        rowMeta.addValueMeta(valueMeta);
      } catch (HopPluginException e) {
        throw new HopTransformException(
            "Unknown data type '"
                + returnValue.getType()
                + "' for value named '"
                + returnValue.getName()
                + "'");
      }
    }
  }
}
