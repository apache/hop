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

package org.apache.hop.execution.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.caching.BaseCachingExecutionInfoLocation;
import org.apache.hop.execution.caching.CacheEntry;
import org.apache.hop.execution.caching.DatedId;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

@GuiPlugin(description = "Elastic execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "elastic-location",
    name = "Elastic location",
    description = "Aggregates and caches execution information before storing in Elastic")
@Getter
@Setter
public class ElasticExecutionInfoLocation extends BaseCachingExecutionInfoLocation
    implements IExecutionInfoLocation {
  public static final Class<?> PKG = ElasticExecutionInfoLocation.class;

  @GuiWidgetElement(
      id = "url",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n:org.apache.hop.execution.elastic:ElasticExecutionInfoLocation.Url.Tooltip",
      label = "i18n:org.apache.hop.execution.elastic:ElasticExecutionInfoLocation.Url.Label")
  @HopMetadataProperty
  protected String url;

  @GuiWidgetElement(
      id = "apiKey",
      order = "020",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      toolTip = "i18n:org.apache.hop.execution.elastic:ElasticExecutionInfoLocation.ApiKey.Tooltip",
      label = "i18n:org.apache.hop.execution.elastic:ElasticExecutionInfoLocation.ApiKey.Label")
  @HopMetadataProperty
  protected String apiKey;

  @GuiWidgetElement(
      id = "indexName",
      order = "030",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip =
          "i18n:org.apache.hop.execution.elastic:ElasticExecutionInfoLocation.IndexName.Tooltip",
      label = "i18n:org.apache.hop.execution.elastic:ElasticExecutionInfoLocation.IndexName.Label")
  @HopMetadataProperty
  protected String indexName;

  protected String actualUrl;
  protected String actualApiKey;
  protected String actualIndexName;

  public ElasticExecutionInfoLocation() {
    super();
  }

  public ElasticExecutionInfoLocation(ElasticExecutionInfoLocation location) {
    super(location);
    this.apiKey = location.apiKey;
    this.actualApiKey = location.actualApiKey;
    this.indexName = location.indexName;
    this.actualIndexName = location.actualIndexName;
    this.url = location.url;
    this.actualUrl = location.actualUrl;
  }

  @Override
  public ElasticExecutionInfoLocation clone() {
    return new ElasticExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    // The actual API Key and Index name to use
    //
    actualUrl = variables.resolve(url);
    actualApiKey = variables.resolve(apiKey);
    actualIndexName = variables.resolve(indexName);

    super.initialize(variables, metadataProvider);
  }

  @Override
  protected void persistCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      // Before writing to disk, we calculate some summaries for convenience of other tools.
      cacheEntry.calculateSummary();

      HttpClient client = HttpClient.newHttpClient();
      URI uri = URI.create(actualUrl);
      URI postUri;

      if (StringUtils.isEmpty(cacheEntry.getInternalId())) {
        postUri = uri.resolve(actualIndexName + "/_doc");
      } else {
        postUri = uri.resolve(actualIndexName + "/_doc/" + cacheEntry.getInternalId());
      }

      ObjectMapper mapper = new ObjectMapper();
      String body = mapper.writeValueAsString(cacheEntry);
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(postUri)
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .header("Authorization", "ApiKey " + actualApiKey)
              .POST(HttpRequest.BodyPublishers.ofByteArray(body.getBytes(StandardCharsets.UTF_8)))
              .build();

      // Send to Elastic and get the response.
      //
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      // Verify the 201 from Elastic
      //
      if (response.statusCode() != 201 && response.statusCode() != 200) {
        throw new HopException(
            "Status code "
                + response.statusCode()
                + " received from Elastic with response: "
                + response.body());
      }

      // Get the _id from the body
      //
      JSONParser parser = new JSONParser();
      JSONObject bodyJs = (JSONObject) parser.parse(response.body());
      String elasticId = (String) bodyJs.get("_id");

      // Keep it around in the cache entry
      //
      cacheEntry.setInternalId(elasticId);

      cacheEntry.setLastWritten(new Date());
    } catch (Exception e) {
      throw new HopException("Error writing caching file entry to Elastic", e);
    }
  }

  @Override
  public void deleteCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      // We first search the document ID with the executionId.
      // Then we perform a DELETE /index-name/_doc/_id
      //
      // Search for the document with the given executionId
      //
      HttpClient client = HttpClient.newHttpClient();
      URI uri = URI.create(actualUrl);
      URI postUri = uri.resolve(actualIndexName + "/_search");

      String body =
          """
                    {
                      "query": {
                        "query_string": {
                          "query": "executionId"
                        }
                      }, "_source": false
                    }
            """;

      body = body.replace("executionId", cacheEntry.getId());

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(postUri)
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .header("Authorization", "ApiKey " + actualApiKey)
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      // Send to Elastic and get the response.
      //
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      // Get the _id from the response body
      //
      JSONParser parser = new JSONParser();
      JSONObject j = (JSONObject) parser.parse(response.body());
      JSONObject jHitsTop = (JSONObject) j.get("hits");
      if (jHitsTop == null) {
        // Nothing left to do, something went wrong
        return;
      }
      JSONArray jHits = (JSONArray) jHitsTop.get("hits");
      if (Utils.isEmpty(jHits)) {
        // No hits returned
        return;
      }

      // Delete all the hits (in case some duplicates were introduced)
      //
      for (int i = 0; i < jHits.size(); i++) {
        JSONObject jHit = (JSONObject) jHits.get(i);
        String elasticId = (String) jHit.get("_id");

        URI deleteUri = uri.resolve(actualIndexName + "/_doc/" + elasticId);
        HttpRequest deleteRequest =
            HttpRequest.newBuilder()
                .uri(deleteUri)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "ApiKey " + actualApiKey)
                .DELETE()
                .build();
        HttpResponse<String> deleteResponse =
            client.send(deleteRequest, HttpResponse.BodyHandlers.ofString());
        if (deleteResponse.statusCode() != 200) {
          throw new HopException(
              "Unable to delete Elastic document with _id : '"
                  + elasticId
                  + "', status code : "
                  + deleteResponse.statusCode());
        }
      }
    } catch (Exception e) {
      throw new HopException("Error deleting caching file entry from Elastic", e);
    }
  }

  protected synchronized CacheEntry loadCacheEntry(String executionId) throws HopException {
    try {
      // Search for the document with the given executionId
      //
      HttpClient client = HttpClient.newHttpClient();
      URI uri = URI.create(actualUrl);
      URI postUri = uri.resolve(actualIndexName + "/_search");

      String body =
          """
                {
                   "query": {
                     "match": {
                       "id": "executionId"
                     }
                   }
                 }
            """;

      body = body.replace("executionId", executionId);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(postUri)
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .header("Authorization", "ApiKey " + actualApiKey)
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      // Send to Elastic and get the response.
      //
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      // Now parse the returned JSON
      //
      JSONParser parser = new JSONParser();
      JSONObject j = (JSONObject) parser.parse(response.body());
      JSONObject jHitsTop = (JSONObject) j.get("hits");
      if (jHitsTop == null) {
        // Nothing left to do, something went wrong
        return null;
      }
      JSONArray jHits = (JSONArray) jHitsTop.get("hits");
      if (Utils.isEmpty(jHits)) {
        // No hits returned
        return null;
      }

      JSONObject jHit = (JSONObject) jHits.get(0);
      JSONObject jSource = (JSONObject) jHit.get("_source");
      if (jSource == null) {
        return null;
      }

      // Convert this source object to CacheEntry
      //
      String json = jSource.toJSONString();
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(json, CacheEntry.class);
    } catch (Exception e) {
      throw new HopException(
          "Error loading execution information location from Elastic using executionId '"
              + executionId
              + "'",
          e);
    }
  }

  @Override
  protected void retrieveIds(boolean includeChildren, Set<DatedId> ids, int limit)
      throws HopException {
    // Get all the IDs from Elastic if we don't have it in the cache.
    //
    try {
      HttpClient client = HttpClient.newHttpClient();
      URI uri = URI.create(actualUrl);
      URI postUri = uri.resolve(actualIndexName + "/_search");

      String body =
          """
            {
              __LIMIT_CLAUSE__
              "from": 0,
              "query" : { "match_all" : {} },
              "fields": [ "id", "execution.executionStartDate" ],
              "sort" : [ { "execution.executionStartDate" : {"order" : "desc" }} ],
              "_source": false
            }
          """;
      String limitClause = "";
      if (limit > 0) {
        limitClause = "\"size\": " + limit + ",";
      }
      body = body.replace("__LIMIT_CLAUSE__", limitClause);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(postUri)
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .header("Authorization", "ApiKey " + actualApiKey)
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      // Send to Elastic and get the response.
      //
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      // Verify the 200 from Elastic
      //
      if (response.statusCode() != 200) {
        throw new HopException(
            "Status code "
                + response.statusCode()
                + " received from Elastic with response: "
                + response.body());
      }

      // Now parse the returned JSON
      //
      JSONParser parser = new JSONParser();
      JSONObject j = (JSONObject) parser.parse(response.body());
      JSONObject jHitsTop = (JSONObject) j.get("hits");
      if (jHitsTop == null) {
        // Nothing left to do, something went wrong
        return;
      }
      JSONArray jHits = (JSONArray) jHitsTop.get("hits");
      if (Utils.isEmpty(jHits)) {
        // No hits returned
        return;
      }

      // This array contains the results wrapped in its own structure.
      // The element fields.id[0] contains the result
      //
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      for (Object hit : jHits) {
        JSONObject jHit = (JSONObject) hit;
        JSONObject jHitsFields = (JSONObject) jHit.get("fields");

        JSONArray jHitsFieldsIds = (JSONArray) jHitsFields.get("id");
        if (Utils.isEmpty(jHitsFieldsIds)) {
          // Skip this one
          continue;
        }
        String id = (String) jHitsFieldsIds.get(0);
        JSONArray jHitsFieldsStart = (JSONArray) jHitsFields.get("execution.executionStartDate");
        if (jHitsFieldsStart != null && !jHitsFieldsStart.isEmpty()) {
          String startDate = (String) jHitsFieldsStart.get(0);
          // Add the dated id
          ids.add(new DatedId(id, sdf.parse(startDate)));
        }
      }
    } catch (Exception e) {
      throw new HopException("Error finding execution ids from Elastic", e);
    }
  }

  /** A button to create and configure the specified index */
  @GuiWidgetElement(
      id = "createIndexButton",
      order = "035",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::ElasticExecutionInfoLocation.CreateIndex.Label",
      toolTip = "i18n::ElasticExecutionInfoLocation.CreateIndex.Tooltip")
  public void createIndexButton(Object object) {
    HopGui hopGui = HopGui.getInstance();
    ElasticExecutionInfoLocation location = (ElasticExecutionInfoLocation) object;

    try {
      // Resolve variables and so on
      //
      location.initialize(hopGui.getVariables(), hopGui.getMetadataProvider());

      HttpClient client = HttpClient.newHttpClient();
      URI uri = URI.create(location.actualUrl);
      URI createUri = uri.resolve(location.actualIndexName);

      // We add a bunch of settings when we create the index
      //
      String createBody =
          """
            {
              "mappings" : {
                "properties": {
                  "id": { "type" : "text"},
                  "name": { "type" : "text"},
                  "execution.id": { "type" : "text"},
                  "execution.name": { "type" : "text"},
                  "execution.filename": { "type" : "text"},
                  "execution.executionType": { "type" : "text"},
                  "execution.parentId": { "type" : "text"},
                  "execution.registrationDate": { "type": "date" },
                  "execution.executionStartDate": { "type": "date" },
                  "executionState.updateTime": { "type": "date" },
                  "executionState.executionEndDate": { "type": "date" },
                  "childExecutions": { "type": "object", "enabled" : false },
                  "childExecutionStates": { "type": "object", "enabled" : false },
                  "childExecutionData": { "type": "object", "enabled" : false }
                }
              }, "settings": {
                "index.mapping.total_fields.limit": 500
              }
            }
          """;

      HttpRequest createRequest =
          HttpRequest.newBuilder()
              .uri(createUri)
              .header("Content-Type", "application/json")
              .header("Accept", "application/json")
              .header("Authorization", "ApiKey " + location.actualApiKey)
              .PUT(HttpRequest.BodyPublishers.ofString(createBody))
              .build();

      // Send to Elastic and we don't care about the response.
      // A 400 usually means that the index already exists.
      //
      HttpResponse<String> createResponse =
          client.send(createRequest, HttpResponse.BodyHandlers.ofString());

      // Verify the 200 from Elastic
      //
      if (createResponse.statusCode() != 200) {
        throw new HopException(
            "Status code "
                + createResponse.statusCode()
                + " received from Elastic with response: "
                + createResponse.body());
      }

      // All done.  The index exists and is configured.
      //
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK);
      box.setText("Index ready");
      box.setMessage(
          BaseMessages.getString(
              PKG,
              "ElasticExecutionInfoLocation.IndexCreatedAndConfigured",
              location.actualIndexName));

      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error creating Elastic index " + location.indexName, e);
    }
  }

  @Override
  public String getPluginId() {
    return "elastic-location";
  }

  public void setPluginId(String pluginId) {
    // Don't set anything
  }

  @Override
  public String getPluginName() {
    return "Elastic location";
  }

  @Override
  public void setPluginName(String pluginName) {
    // Nothing to set
  }
}
