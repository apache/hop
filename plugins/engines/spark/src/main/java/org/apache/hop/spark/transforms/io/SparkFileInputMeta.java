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

package org.apache.hop.spark.transforms.io;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.util.SparkConst;

@Transform(
    id = SparkConst.SPARK_FILE_INPUT_PLUGIN_ID,
    name = "i18n::SparkFileInput.Name",
    description = "i18n::SparkFileInput.Description",
    image = "spark-file-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::SparkFileInput.Keyword",
    documentationUrl = "/pipeline/transforms/spark-file-input.html",
    supportedEngines = {SparkConst.PLUGIN_ID})
@Getter
@Setter
public class SparkFileInputMeta extends BaseTransformMeta<SparkFileInput, SparkFileInputData> {

  public static final String FORMAT_CSV = "csv";
  public static final String FORMAT_PARQUET = "parquet";
  public static final String FORMAT_JSON = "json";
  public static final String FORMAT_ORC = "orc";
  public static final String FORMAT_TEXT = "text";

  @HopMetadataProperty(key = "file_path")
  private String filePath;

  /** csv, parquet, json, orc, text */
  @HopMetadataProperty(key = "file_format")
  private String fileFormat = FORMAT_CSV;

  @HopMetadataProperty(key = "header")
  private boolean header = true;

  @HopMetadataProperty(key = "separator")
  private String separator = ",";

  @HopMetadataProperty(key = "quote")
  private String quote = "\"";

  @HopMetadataProperty(key = "infer_schema")
  private boolean inferSchema = false;

  @HopMetadataProperty(key = "multi_line")
  private boolean multiLine = false;

  /** Extra Spark read options as key=value lines */
  @HopMetadataProperty(key = "extra_options")
  private String extraOptions;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<SparkField> fields = new ArrayList<>();

  public SparkFileInputMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return SparkFileInputDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    inputRowMeta.clear();
    if (fields == null || fields.isEmpty()) {
      return;
    }
    try {
      for (SparkField field : fields) {
        if (field.getName() != null && !field.getName().isEmpty()) {
          inputRowMeta.addValueMeta(field.createValueMeta());
        }
      }
    } catch (HopPluginException e) {
      throw new HopTransformException("Unable to create row meta for Spark File Input", e);
    }
  }

  public IRowMeta createRowMeta() throws HopPluginException {
    org.apache.hop.core.row.RowMeta rowMeta = new org.apache.hop.core.row.RowMeta();
    if (fields != null) {
      for (SparkField field : fields) {
        if (field.getName() != null && !field.getName().isEmpty()) {
          rowMeta.addValueMeta(field.createValueMeta());
        }
      }
    }
    return rowMeta;
  }
}
