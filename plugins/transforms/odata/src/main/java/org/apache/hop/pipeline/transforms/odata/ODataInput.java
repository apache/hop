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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ODataInput extends BaseTransform<ODataInputMeta, ODataInputData> {
  private static final Class<?> PKG = ODataInputMeta.class;

  public ODataInput(
      TransformMeta transformMeta,
      ODataInputMeta meta,
      ODataInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (super.init()) {
      try {
        HttpClientManager.HttpClientBuilderFacade builder =
            HttpClientManager.getInstance().createBuilder();
        if ("BASIC".equalsIgnoreCase(meta.getAuthType()) && !Utils.isEmpty(meta.getUsername())) {
          builder.setCredentials(resolve(meta.getUsername()), resolve(meta.getPassword()));
        }
        data.httpClient = builder.build();

        String serviceUrl = resolve(meta.getUrl());
        String entitySet = resolve(meta.getEntitySet());
        if (Utils.isEmpty(serviceUrl)) {
          logError("Service URL is empty.");
          return false;
        }
        if (Utils.isEmpty(entitySet)) {
          logError("Entity Set is empty.");
          return false;
        }

        String rootUrl = serviceUrl;
        if (!rootUrl.endsWith("/")) {
          rootUrl += "/";
        }
        String requestUrl = rootUrl + entitySet;

        List<String> queryParams = new ArrayList<>();
        if (!Utils.isEmpty(meta.getQuerySelect())) {
          queryParams.add(
              "$select="
                  + URLEncoder.encode(
                      resolve(meta.getQuerySelect()), StandardCharsets.UTF_8.name()));
        }
        if (!Utils.isEmpty(meta.getQueryFilter())) {
          queryParams.add(
              "$filter="
                  + URLEncoder.encode(
                      resolve(meta.getQueryFilter()), StandardCharsets.UTF_8.name()));
        }
        if (!Utils.isEmpty(meta.getQueryOrder())) {
          queryParams.add(
              "$orderby="
                  + URLEncoder.encode(
                      resolve(meta.getQueryOrder()), StandardCharsets.UTF_8.name()));
        }
        if (!Utils.isEmpty(meta.getQueryTop())) {
          queryParams.add(
              "$top="
                  + URLEncoder.encode(resolve(meta.getQueryTop()), StandardCharsets.UTF_8.name()));
        }
        if (!Utils.isEmpty(meta.getQuerySkip())) {
          queryParams.add(
              "$skip="
                  + URLEncoder.encode(resolve(meta.getQuerySkip()), StandardCharsets.UTF_8.name()));
        }

        if (!queryParams.isEmpty()) {
          requestUrl += "?" + String.join("&", queryParams);
        }
        data.nextPageUrl = requestUrl;
        data.isFinishedReading = false;

        return true;
      } catch (Exception e) {
        logError("Error initializing OData HTTP Client", e);
        return false;
      }
    }
    return false;
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    if (data.recordIndex < data.recordBuffer.size()) {
      Object[] row = data.recordBuffer.get(data.recordIndex++);
      putRow(data.outputRowMeta, row);
      return true;
    }

    if (data.isFinishedReading && data.recordIndex >= data.recordBuffer.size()) {
      setOutputDone();
      return false;
    }

    // Fetch next page of data
    fetchNextPage();

    if (data.recordBuffer.isEmpty()) {
      setOutputDone();
      return false;
    }

    Object[] row = data.recordBuffer.get(data.recordIndex++);
    putRow(data.outputRowMeta, row);
    return true;
  }

  private void fetchNextPage() throws HopException {
    if (data.nextPageUrl == null || data.nextPageUrl.isEmpty()) {
      data.isFinishedReading = true;
      return;
    }

    try {
      HttpGet get = new HttpGet(data.nextPageUrl);
      get.setHeader("Accept", "application/json");
      if ("BEARER".equalsIgnoreCase(meta.getAuthType()) && !Utils.isEmpty(meta.getToken())) {
        get.setHeader("Authorization", "Bearer " + resolve(meta.getToken()));
      }

      try (CloseableHttpResponse response = data.httpClient.execute(get)) {
        int statusCode = response.getCode();
        if (statusCode != 200) {
          throw new HopException("OData service returned non-200 status code: " + statusCode);
        }

        HttpEntity entity = response.getEntity();
        String body = entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(body);

        JsonNode recordsNode = null;
        String nextLink = null;

        if (rootNode.has("value")) {
          // OData V4
          recordsNode = rootNode.get("value");
          if (rootNode.has("@odata.nextLink")) {
            nextLink = rootNode.get("@odata.nextLink").asText();
          }
        } else if (rootNode.has("d")) {
          // OData V2
          JsonNode dNode = rootNode.get("d");
          if (dNode.has("results")) {
            recordsNode = dNode.get("results");
          } else if (dNode.isArray()) {
            recordsNode = dNode;
          } else {
            // Single object wrapper
            recordsNode = mapper.createArrayNode();
            ((com.fasterxml.jackson.databind.node.ArrayNode) recordsNode).add(dNode);
          }

          if (dNode.has("__next")) {
            nextLink = dNode.get("__next").asText();
          }
        } else {
          // General json backup
          if (rootNode.isArray()) {
            recordsNode = rootNode;
          } else {
            recordsNode = mapper.createArrayNode();
            ((com.fasterxml.jackson.databind.node.ArrayNode) recordsNode).add(rootNode);
          }
        }

        data.recordBuffer.clear();
        data.recordIndex = 0;

        if (recordsNode != null && recordsNode.isArray()) {
          for (JsonNode record : recordsNode) {
            Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
            for (int i = 0; i < meta.getFields().size(); i++) {
              ODataField field = meta.getFields().get(i);
              String path = resolve(field.getPath());
              JsonNode valueNode = getValueByPath(record, path);
              IValueMeta valueMeta = data.outputRowMeta.getValueMeta(i);

              Object val = null;
              if (valueNode != null && !valueNode.isNull()) {
                if (valueMeta.isBoolean()) {
                  val = valueNode.asBoolean();
                } else if (valueMeta.isInteger()) {
                  val = valueNode.asLong();
                } else if (valueMeta.isNumber()) {
                  val = valueNode.asDouble();
                } else if (valueMeta.isDate()) {
                  String text = valueNode.asText();
                  try {
                    val = valueMeta.convertDataFromString(text, null, null, null, 0);
                  } catch (Exception ex) {
                    logError("Failed to parse date: " + text + ", field: " + field.getName(), ex);
                  }
                } else {
                  val = valueNode.asText();
                }
              }
              row[i] = val;
            }
            data.recordBuffer.add(row);
          }
        }

        if (nextLink != null && !nextLink.isEmpty()) {
          // Resolve nextLink relative to base if needed
          if (!nextLink.startsWith("http://") && !nextLink.startsWith("https://")) {
            URI uri = URI.create(data.nextPageUrl);
            String portStr = uri.getPort() != -1 ? ":" + uri.getPort() : "";
            String base = uri.getScheme() + "://" + uri.getHost() + portStr;
            if (nextLink.startsWith("/")) {
              nextLink = base + nextLink;
            } else {
              String path = uri.getPath();
              int idx = path.lastIndexOf("/");
              if (idx != -1) {
                nextLink = base + path.substring(0, idx + 1) + nextLink;
              } else {
                nextLink = base + "/" + nextLink;
              }
            }
          }
          data.nextPageUrl = nextLink;
        } else {
          data.nextPageUrl = null;
          data.isFinishedReading = true;
        }
      }
    } catch (Exception e) {
      throw new HopException("Error requesting OData data page", e);
    }
  }

  private JsonNode getValueByPath(JsonNode root, String path) {
    if (root == null || path == null || path.isEmpty()) {
      return root;
    }
    String[] parts = path.split("/");
    JsonNode current = root;
    for (String part : parts) {
      if (current == null) {
        return null;
      }
      current = current.get(part);
    }
    return current;
  }

  @Override
  public void dispose() {
    if (data.httpClient != null) {
      try {
        data.httpClient.close();
      } catch (Exception e) {
        logError("Error closing OData HTTP Client", e);
      }
    }
    super.dispose();
  }
}
