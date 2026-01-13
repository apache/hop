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

package org.apache.hop.pipeline.transforms.sasinput;

import com.epam.parso.Column;
import com.epam.parso.ColumnFormat;
import com.epam.parso.impl.SasFileReaderImpl;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SASInput",
    image = "sasinput.svg",
    name = "i18n::SasInput.Transform.Name",
    description = "i18n::SasInput.Transform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::SasInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/sasinput.html")
@Getter
@Setter
public class SasInputMeta extends BaseTransformMeta<SasInput, SasInputData> {
  private static final Class<?> PKG = SasInputMeta.class; // for i18n purposes,

  /** The field in which the filename is placed */
  @HopMetadataProperty(key = "accept_field")
  private String acceptingField;

  @HopMetadataProperty(key = "field")
  private List<SasInputField> outputFields;

  @HopMetadataProperty(key = "meta_filename")
  private String metadataFilename;

  @HopMetadataProperty private String limit;

  public SasInputMeta() {
    super(); // allocate BaseTransformMeta
    outputFields = new ArrayList<>();
  }

  public SasInputMeta(SasInputMeta m) {
    this();
    this.acceptingField = m.acceptingField;
    this.metadataFilename = m.metadataFilename;
    this.limit = m.limit;
    for (SasInputField field : m.outputFields) {
      outputFields.add(new SasInputField(field));
    }
  }

  @Override
  public SasInputMeta clone() {
    return new SasInputMeta(this);
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

    String metaFilename = variables.resolve(metadataFilename);
    if (StringUtils.isEmpty(metaFilename)) {

      for (SasInputField field : outputFields) {
        try {
          IValueMeta valueMeta =
              ValueMetaFactory.createValueMeta(field.getRename(), field.getType());
          valueMeta.setLength(field.getLength(), field.getPrecision());
          valueMeta.setDecimalSymbol(field.getDecimalSymbol());
          valueMeta.setGroupingSymbol(field.getGroupingSymbol());
          valueMeta.setConversionMask(field.getConversionMask());
          valueMeta.setTrimType(field.getTrimType());
          valueMeta.setOrigin(name);

          inputRowMeta.addValueMeta(valueMeta);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    } else {
      // We need to get the file metadata from a reference file to get the row layout.
      try (InputStream inputStream = HopVfs.getInputStream(metaFilename, variables)) {
        SasFileReaderImpl sasFileReader = new SasFileReaderImpl(inputStream);

        List<Column> columns = sasFileReader.getColumns();
        for (Column column : columns) {
          ColumnFormat format = column.getFormat();

          String columnName = column.getName();
          int length = format.getWidth() == 0 ? -1 : format.getWidth();
          int precision = format.getPrecision() == 0 ? -1 : format.getWidth();
          int columnType = SasUtil.getHopDataType(column.getType());
          IValueMeta valueMeta =
              ValueMetaFactory.createValueMeta(columnName, columnType, length, precision);
          valueMeta.setOrigin(name);
          inputRowMeta.addValueMeta(valueMeta);
        }
      } catch (Exception e) {
        throw new HopTransformException("Error reading from metadata file: " + metaFilename);
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

    CheckResult cr;

    if (Utils.isEmpty(getAcceptingField())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SASInput.Log.Error.InvalidAcceptingFieldName"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
