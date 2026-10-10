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

package org.apache.hop.neo4j.transforms.schema;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Reads the schema of a graph database and writes one row per label or type and property. */
public class GetGraphSchema extends BaseTransform<GetGraphSchemaMeta, GetGraphSchemaData> {

  private static final Class<?> PKG = GetGraphSchemaMeta.class;

  public GetGraphSchema(
      TransformMeta transformMeta,
      GetGraphSchemaMeta meta,
      GetGraphSchemaData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (Utils.isEmpty(meta.getConnection())) {
      logError(BaseMessages.getString(PKG, "GetGraphSchema.Error.NoConnection"));
      return false;
    }
    String sampleSize = resolve(meta.getSampleSize());
    data.sampleSize =
        Utils.isEmpty(sampleSize) ? GraphSchema.DEFAULT_SAMPLE_SIZE : Const.toInt(sampleSize, -1);
    if (data.sampleSize < 1) {
      logError(BaseMessages.getString(PKG, "GetGraphSchema.Error.SampleSize", sampleSize));
      return false;
    }
    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {
    data.outputRowMeta = new RowMeta();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, getMetadataProvider());

    NamedGraphConnection graphConnection =
        NeoConnectionUtils.getGraphConnection(getMetadataProvider(), resolve(meta.getConnection()));
    IGraphDialect dialect = graphConnection.getDialect(this);
    if (!dialect.isSupportingSchemaIntrospection()) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "GetGraphSchema.Error.NotSupported", graphConnection.name(), dialect.getId()));
    }
    GraphSchema schema;
    try (IGraphConnection connection = graphConnection.connect(getLogChannel(), this)) {
      schema = connection.getSchema(data.sampleSize);
    }
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              schema.sampled() ? "GetGraphSchema.Log.Sampled" : "GetGraphSchema.Log.Catalog",
              Integer.toString(schema.entries().size()),
              Integer.toString(data.sampleSize)));
    }
    for (Object[] row : getRows(meta, schema)) {
      putRow(data.outputRowMeta, row);
      if (isStopped()) {
        break;
      }
    }
    setOutputDone();
    return false;
  }

  /** The output rows of a schema, with the fields of the transform which have a name. */
  static List<Object[]> getRows(GetGraphSchemaMeta meta, GraphSchema schema) {
    List<Object[]> rows = new ArrayList<>();
    for (GraphSchemaEntry entry : schema.entries()) {
      List<Object> values = new ArrayList<>();
      add(values, meta.getElementTypeField(), entry.elementType().name());
      add(values, meta.getNameField(), entry.name());
      add(values, meta.getPropertyField(), entry.property());
      add(values, meta.getPropertyTypesField(), join(entry.propertyTypes()));
      add(values, meta.getMandatoryField(), entry.mandatory());
      add(values, meta.getIndexedField(), schema.isIndexed(entry));
      add(values, meta.getUniqueField(), schema.isUnique(entry));
      add(values, meta.getStartLabelsField(), join(entry.startLabels()));
      add(values, meta.getEndLabelsField(), join(entry.endLabels()));
      rows.add(values.toArray());
    }
    return rows;
  }

  private static void add(List<Object> values, String fieldName, Object value) {
    if (!Utils.isEmpty(fieldName)) {
      values.add(value);
    }
  }

  private static String join(List<String> values) {
    return values.isEmpty() ? null : String.join(",", values);
  }
}
