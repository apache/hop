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

package org.apache.hop.pipeline.transforms.mongodbinput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 8-apr-2011
 *
 * @since 4.2.0-M1
 */
@Transform(
    id = "MongoDbInput",
    image = "mongodb-input.svg",
    name = "i18n::MongoDbInput.Name",
    description = "i18n::MongoDbInput.Description",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/mongodbinput.html",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input")
@InjectionSupported(localizationPrefix = "MongoDbInput.Injection.", groups = ("FIELDS"))
public class MongoDbInputMeta extends MongoDbMeta<MongoDbInput, MongoDbInputData>
    implements ITransformMeta<MongoDbInput, MongoDbInputData> {
  protected static Class<?> PKG = MongoDbInputMeta.class; // For Translator

  @Injection(name = "JSON_OUTPUT_FIELD")
  private String jsonFieldName;

  @Injection(name = "JSON_FIELD")
  private String jsonField;

  @Injection(name = "JSON_QUERY")
  private String jsonQuery;

  @Injection(name = "AGG_PIPELINE")
  private boolean aggPipeline = false;

  @Injection(name = "OUTPUT_JSON")
  private boolean outputJson = true;

  @InjectionDeep private List<MongoField> fields;

  @Injection(name = "EXECUTE_FOR_EACH_ROW")
  private boolean executeForEachIncomingRow = false;

  public void setMongoFields(List<MongoField> fields) {
    this.fields = fields;
  }

  public List<MongoField> getMongoFields() {
    return fields;
  }

  public void setExecuteForEachIncomingRow(boolean e) {
    executeForEachIncomingRow = e;
  }

  public boolean getExecuteForEachIncomingRow() {
    return executeForEachIncomingRow;
  }

  @Override
  public void loadXml(Node node, IHopMetadataProvider metaStore) throws HopXmlException {
    try {
      connectionName = XmlHandler.getTagValue(node, "connection");
      jsonField = XmlHandler.getTagValue(node, "fields_name");
      collection = XmlHandler.getTagValue(node, "collection");
      jsonFieldName = XmlHandler.getTagValue(node, "json_field_name");
      jsonQuery = XmlHandler.getTagValue(node, "json_query");

      outputJson = true; // default to true for backwards compatibility
      String outputJson = XmlHandler.getTagValue(node, "output_json");
      if (!StringUtils.isEmpty(outputJson)) {
        this.outputJson = outputJson.equalsIgnoreCase("Y");
      }

      String queryIsPipe = XmlHandler.getTagValue(node, "query_is_pipeline");
      if (!StringUtils.isEmpty(queryIsPipe)) {
        aggPipeline = queryIsPipe.equalsIgnoreCase("Y");
      }

      String executeForEachR = XmlHandler.getTagValue(node, "execute_for_each_row");
      if (!StringUtils.isEmpty(executeForEachR)) {
        executeForEachIncomingRow = executeForEachR.equalsIgnoreCase("Y");
      }

      Node mongo_fields = XmlHandler.getSubNode(node, "mongo_fields");
      if (mongo_fields != null && XmlHandler.countNodes(mongo_fields, "mongo_field") > 0) {
        int nrFields = XmlHandler.countNodes(mongo_fields, "mongo_field");

        fields = new ArrayList<>();
        for (int i = 0; i < nrFields; i++) {
          Node fieldNode = XmlHandler.getSubNodeByNr(mongo_fields, "mongo_field", i);

          MongoField newField = new MongoField();
          newField.fieldName = XmlHandler.getTagValue(fieldNode, "field_name");
          newField.fieldPath = XmlHandler.getTagValue(fieldNode, "field_path");
          newField.hopType = XmlHandler.getTagValue(fieldNode, "field_type");
          String indexedVals = XmlHandler.getTagValue(fieldNode, "indexed_vals");
          if (indexedVals != null && indexedVals.length() > 0) {
            newField.indexedValues = MongoDbInputData.indexedValsList(indexedVals);
          }
          fields.add(newField);
        }
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MongoDbInputMeta.Exception.UnableToLoadTransformInfo"), e);
    }
  }

  @Override
  public Object clone() {
    MongoDbInputMeta meta = (MongoDbInputMeta) super.clone();
    return meta;
  }

  @Override
  public void setDefault() {
    jsonFieldName = "json";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    try {
      if (outputJson || fields == null || fields.size() == 0) {
        IValueMeta jsonValueMeta =
            ValueMetaFactory.createValueMeta(jsonFieldName, IValueMeta.TYPE_STRING);
        jsonValueMeta.setOrigin(origin);
        rowMeta.addValueMeta(jsonValueMeta);
      } else {
        for (MongoField f : fields ) {
          int type = ValueMetaFactory.getIdForValueMeta(f.hopType);
          IValueMeta vm = ValueMetaFactory.createValueMeta(f.fieldName, type);
          vm.setOrigin(origin);
          if (f.indexedValues != null) {
            vm.setIndex(f.indexedValues.toArray()); // indexed values
          }
          rowMeta.addValueMeta(vm);
        }
      }
    } catch (Exception e) {
      throw new HopTransformException("Error processing output fields of transform", e);
    }
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder(300);

    xml.append("    ").append(XmlHandler.addTagValue("connection", connectionName));
    xml.append("    ").append(XmlHandler.addTagValue("fields_name", jsonField ));
    xml.append("    ").append(XmlHandler.addTagValue("collection", collection));
    xml.append("    ").append(XmlHandler.addTagValue("json_field_name", jsonFieldName));
    xml.append("    ").append(XmlHandler.addTagValue("json_query", jsonQuery));
    xml.append("    ").append(XmlHandler.addTagValue("output_json", outputJson));
    xml.append("    ").append(XmlHandler.addTagValue("query_is_pipeline", aggPipeline));
    xml.append("    ")
        .append(XmlHandler.addTagValue("execute_for_each_row", executeForEachIncomingRow));

    if ( fields != null && fields.size() > 0) {
      xml.append("\n    ").append(XmlHandler.openTag("mongo_fields"));

      for (MongoField f : fields ) {
        xml.append("\n      ").append(XmlHandler.openTag("mongo_field"));

        xml.append("\n        ").append(XmlHandler.addTagValue("field_name", f.fieldName));
        xml.append("\n        ").append(XmlHandler.addTagValue("field_path", f.fieldPath));
        xml.append("\n        ").append(XmlHandler.addTagValue("field_type", f.hopType));
        if (f.indexedValues != null && f.indexedValues.size() > 0) {
          xml.append("\n        ")
              .append(
                  XmlHandler.addTagValue(
                      "indexed_vals", MongoDbInputData.indexedValsList(f.indexedValues)));
        }
        xml.append("\n      ").append(XmlHandler.closeTag("mongo_field"));
      }

      xml.append("\n    ").append(XmlHandler.closeTag("mongo_fields"));
    }
    return xml.toString();
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      MongoDbInputData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new MongoDbInput(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public MongoDbInputData getTransformData() {
    return new MongoDbInputData();
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
    super.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);
  }

  /** @return the fields */
  public String getFieldsName() {
    return jsonField;
  }

  /** @param fields a field name to set */
  public void setFieldsName(String fields) {
    this.jsonField = fields;
  }

  /** @return the jsonFieldName */
  public String getJsonFieldName() {
    return jsonFieldName;
  }

  /** @param jsonFieldName the jsonFieldName to set */
  public void setJsonFieldName(String jsonFieldName) {
    this.jsonFieldName = jsonFieldName;
  }

  /** @return the jsonQuery */
  public String getJsonQuery() {
    return jsonQuery;
  }

  /** @param jsonQuery the jsonQuery to set */
  public void setJsonQuery(String jsonQuery) {
    this.jsonQuery = jsonQuery;
  }

  /**
   * Set whether to output just a single field as JSON
   *
   * @param outputJson true if a single field containing JSON is to be output
   */
  public void setOutputJson(boolean outputJson) {
    this.outputJson = outputJson;
  }

  /**
   * Get whether to output just a single field as JSON
   *
   * @return true if a single field containing JSON is to be output
   */
  public boolean isOutputJson() {
    return outputJson;
  }

  /**
   * Set whether the supplied query is actually a pipeline specification
   *
   * @param q true if the supplied query is a pipeline specification
   */
  public void setQueryIsPipeline(boolean q) {
    aggPipeline = q;
  }

  /**
   * Get whether the supplied query is actually a pipeline specification
   *
   * @true true if the supplied query is a pipeline specification
   */
  public boolean isQueryIsPipeline() {
    return aggPipeline;
  }
}
