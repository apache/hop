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
 *
 */

package org.apache.hop.avro.transforms.avroencode;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "AvroEncode",
    name = "Avro Encode",
    description = "Encodes Hop fields into an Avro Record typed field",
    image = "avro_encode.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/avro-encode.html",
    keywords = "i18n::AvroEncodeMeta.keyword")
public class AvroEncodeMeta extends BaseTransformMeta<AvroEncode, AvroEncodeData> {
  private static final Class<?> PKG = AvroEncodeMeta.class; // For Translator

  @HopMetadataProperty(key = "output_field")
  private String outputFieldName;

  @HopMetadataProperty(key = "schema_name")
  private String schemaName;

  @HopMetadataProperty private String namespace;

  @HopMetadataProperty private String documentation;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<SourceField> sourceFields;

  public AvroEncodeMeta() {
    outputFieldName = "avro";
    sourceFields = new ArrayList<>();
    schemaName = "hop-schema";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String transformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) throws HopTransformException {

    try {
      Schema schema =
          createAvroSchema(
              variables.resolve(getSchemaName()),
              variables.resolve(getNamespace()),
              variables.resolve(getDocumentation()),
              rowMeta,
              sourceFields);
      ValueMetaAvroRecord valueMeta = new ValueMetaAvroRecord(variables.resolve(outputFieldName), schema);
      rowMeta.addValueMeta(valueMeta);
    } catch (Exception e) {
      throw new HopTransformException(
          "Error creating Avro schema and/or determining output field layout", e);
    }
  }

  public static Schema createAvroSchema(
      String name,
      String namespace,
      String doc,
      IRowMeta inputRowMeta,
      List<SourceField> sourceFields)
      throws HopException {
    SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(name);
    if (StringUtils.isNotEmpty(namespace)) {
      recordBuilder = recordBuilder.namespace(namespace);
    }
    if (StringUtils.isNotEmpty(doc)) {
      recordBuilder.doc(doc);
    }

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

    for (SourceField field : sourceFields) {
      int index = inputRowMeta.indexOfValue(field.getSourceFieldName());
      if (index < 0) {
        throw new HopException("Unable to find input field " + field.getSourceFieldName());
      }
      IValueMeta valueMeta = inputRowMeta.getValueMeta(index);

      // Start a new field
      SchemaBuilder.BaseFieldTypeBuilder<Schema> fieldBuilder =
          fieldAssembler.name(field.calculateTargetFieldName()).type().nullable();

      // Add this field to the schema...
      //
      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_DATE:
          fieldAssembler = fieldBuilder.longType().noDefault();
          Schema timestampMilliType =
              LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
          fieldAssembler =
              fieldAssembler
                  .name(field.getTargetFieldName())
                  .type()
                  .unionOf()
                  .nullType()
                  .and()
                  .type(timestampMilliType)
                  .endUnion()
                  .noDefault();
          break;
        case IValueMeta.TYPE_INTEGER:
          fieldAssembler = fieldBuilder.longType().noDefault();
          break;
        case IValueMeta.TYPE_NUMBER:
          fieldAssembler = fieldBuilder.doubleType().noDefault();
          break;
        case IValueMeta.TYPE_BOOLEAN:
          fieldAssembler = fieldBuilder.booleanType().noDefault();
          break;
        case IValueMeta.TYPE_STRING:
        case IValueMeta.TYPE_BIGNUMBER:
          // Convert BigDecimal to String,otherwise we'll have all sorts of conversion issues.
          //
          fieldAssembler = fieldBuilder.stringType().noDefault();
          break;
        case IValueMeta.TYPE_BINARY:
          fieldAssembler = fieldBuilder.bytesType().noDefault();
          break;
        default:
          throw new HopException(
              "Writing Hop data type '"
                  + valueMeta.getTypeDesc()
                  + "' to Parquet is not supported");
      }
    }

    return fieldAssembler.endRecord();
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName(String outputFieldName) {
    this.outputFieldName = outputFieldName;
  }

  public List<SourceField> getSourceFields() {
    return sourceFields;
  }

  public void setSourceFields(List<SourceField> sourceFields) {
    this.sourceFields = sourceFields;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getDocumentation() {
    return documentation;
  }

  public void setDocumentation(String documentation) {
    this.documentation = documentation;
  }
}
