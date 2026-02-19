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

package org.apache.hop.parquet.transforms.input;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

@Getter
@Setter
@Transform(
    id = "ParquetFileInput",
    image = "parquet_input.svg",
    name = "i18n::ParquetInput.Name",
    description = "i18n::ParquetInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/parquet-file-input.html",
    keywords = "i18n::ParquetInputMeta.keyword")
public class ParquetInputMeta extends BaseTransformMeta<ParquetInput, ParquetInputData> {

  @HopMetadataProperty(key = "filename_field")
  private String filenameField;

  @HopMetadataProperty(key = "metadata_filename")
  private String metadataFilename;

  @HopMetadataProperty(key = "nulls_when_empty")
  private boolean sendingNullsRowWhenEmpty;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<ParquetField> fields;

  public ParquetInputMeta() {
    fields = new ArrayList<>();
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

    // If there is a filename from which to extra the field metadata, use this
    //
    if (fields.isEmpty() && StringUtils.isNotEmpty(metadataFilename)) {
      String filename = variables.resolve(metadataFilename);
      try {
        inputRowMeta.addRowMeta(extractRowMeta(variables, filename));
        return;
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }

    // Add the fields to the input
    //
    for (ParquetField field : fields) {
      try {
        IValueMeta valueMeta = field.createValueMeta();
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);
      } catch (HopException e) {
        throw new HopTransformException(
            "Unable to create value metadata of type '" + field.getTargetType() + "'", e);
      }
    }
  }

  public static IRowMeta extractRowMeta(IVariables variables, String filename) throws HopException {
    try {
      FileObject fileObject = HopVfs.getFileObject(variables.resolve(filename), variables);

      long size = fileObject.getContent().getSize();
      InputStream inputStream = HopVfs.getInputStream(fileObject);

      // Reads the whole file into memory...
      //
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) size);
      IOUtils.copy(inputStream, outputStream);
      ParquetStream inputFile = new ParquetStream(outputStream.toByteArray(), filename);
      // Empty list of fields to retrieve: we still grab the schema
      //
      ParquetReadSupport readSupport = new ParquetReadSupport(new ArrayList<>());
      ParquetReader<RowMetaAndData> reader =
          new ParquetReaderBuilder<>(readSupport, inputFile).build();

      // Read one empty row...
      //
      reader.read();

      // Now we have the schema...
      //
      MessageType schema = readSupport.getMessageType();
      IRowMeta rowMeta = new RowMeta();
      List<ColumnDescriptor> columns = schema.getColumns();
      for (ColumnDescriptor column : columns) {
        String sourceField = "";
        String[] path = column.getPath();
        if (path.length == 1) {
          sourceField = path[0];
        } else {
          for (int i = 0; i < path.length; i++) {
            if (i > 0) {
              sourceField += ".";
            }
            sourceField += path[i];
          }
        }
        PrimitiveType primitiveType = column.getPrimitiveType();
        int hopType = IValueMeta.TYPE_STRING;
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (logicalType != null) {
          if ((logicalType instanceof TimestampLogicalTypeAnnotation)
              || (logicalType instanceof TimeLogicalTypeAnnotation)) {
            hopType = IValueMeta.TYPE_TIMESTAMP;
          } else if (logicalType instanceof DateLogicalTypeAnnotation) {
            hopType = IValueMeta.TYPE_DATE;
          } else if (logicalType instanceof JsonLogicalTypeAnnotation) {
            hopType = IValueMeta.TYPE_JSON;
          } else if (logicalType instanceof DecimalLogicalTypeAnnotation) {
            hopType = IValueMeta.TYPE_BIGNUMBER;
          } else if (logicalType instanceof IntLogicalTypeAnnotation) {
            hopType = IValueMeta.TYPE_INTEGER;
          }
        } else {
          hopType =
              switch (primitiveType.getPrimitiveTypeName()) {
                case INT32, INT64 -> IValueMeta.TYPE_INTEGER;
                case INT96 -> IValueMeta.TYPE_BINARY;
                case FLOAT, DOUBLE -> IValueMeta.TYPE_NUMBER;
                case BOOLEAN -> IValueMeta.TYPE_BOOLEAN;
                case BINARY -> IValueMeta.TYPE_BINARY;
                default -> hopType;
              };
        }
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(sourceField, hopType, -1, -1);
        rowMeta.addValueMeta(valueMeta);
      }
      return rowMeta;
    } catch (Exception e) {
      throw new HopException(
          "Unable to extract row metadata from parquet file '" + filename + "'", e);
    }
  }
}
