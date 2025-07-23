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

package org.apache.hop.execution.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpRequest;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.caching.BaseCachingExecutionInfoLocation;
import org.apache.hop.execution.caching.CacheEntry;
import org.apache.hop.execution.caching.DatedId;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

@GuiPlugin(description = "OpenSearch execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "opensearch-location",
    name = "OpenSearch location",
    description = "Aggregates and caches execution information before storing in OpenSearch")
@Getter
@Setter
public class OpenSearchExecutionInfoLocation extends BaseCachingExecutionInfoLocation
    implements IExecutionInfoLocation {
  public static final Class<?> PKG = OpenSearchExecutionInfoLocation.class;

  @GuiWidgetElement(
      id = "url",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.Url.Tooltip",
      label = "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.Url.Label")
  @HopMetadataProperty
  protected String url;

  @GuiWidgetElement(
      id = "username",
      order = "020",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = false,
      toolTip =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.Username.Tooltip",
      label =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.Username.Label")
  @HopMetadataProperty
  protected String username;

  @GuiWidgetElement(
      id = "password",
      order = "030",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      toolTip =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.Password.Tooltip",
      label =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.Password.Label")
  @HopMetadataProperty
  protected String password;

  @GuiWidgetElement(
      id = "indexName",
      order = "040",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.IndexName.Tooltip",
      label =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.IndexName.Label")
  @HopMetadataProperty
  protected String indexName;

  @GuiWidgetElement(
      id = "ignoreSsl",
      order = "050",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      toolTip =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.IgnoreSsl.Tooltip",
      label =
          "i18n:org.apache.hop.execution.opensearch:OpenSearchExecutionInfoLocation.IgnoreSsl.Label")
  @HopMetadataProperty
  protected boolean ignoreSsl;

  protected String actualUrl;
  protected String actualUsername;
  protected String actualPassword;
  protected String actualIndexName;

  public OpenSearchExecutionInfoLocation() {
    super();
  }

  public OpenSearchExecutionInfoLocation(OpenSearchExecutionInfoLocation location) {
    super(location);
    this.url = location.url;
    this.username = location.username;
    this.password = location.password;
    this.indexName = location.indexName;
    this.ignoreSsl = location.ignoreSsl;

    this.actualUrl = location.actualUrl;
    this.actualIndexName = location.actualIndexName;
    this.actualUsername = location.actualUsername;
    this.actualPassword = location.actualPassword;
  }

  @Override
  public OpenSearchExecutionInfoLocation clone() {
    return new OpenSearchExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    // The actual API Key and Index name to use
    //
    actualUrl = variables.resolve(url);
    actualUsername = variables.resolve(username);
    actualPassword = variables.resolve(password);
    actualIndexName = variables.resolve(indexName);

    super.initialize(variables, metadataProvider);
  }

  @Override
  protected void persistCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      // Before writing to disk, we calculate some summaries for convenience of other tools.
      cacheEntry.calculateSummary();

      URI uri = URI.create(actualUrl);
      URI postUri;

      if (StringUtils.isEmpty(cacheEntry.getInternalId())) {
        postUri = uri.resolve(actualIndexName + "/_doc");
      } else {
        postUri = uri.resolve(actualIndexName + "/_doc/" + cacheEntry.getInternalId());
      }

      ObjectMapper mapper = new ObjectMapper();
      String body = mapper.writeValueAsString(cacheEntry);

      RestCaller restCaller =
          new RestCaller(
              metadataProvider,
              postUri.toString(),
              actualUsername,
              actualPassword,
              "POST",
              body,
              ignoreSsl,
              getHeaders());

      String responseBody = restCaller.execute();
      Long statusCode = restCaller.getStatusCode();

      // Verify the 200/201 from OpenSearch
      //
      checkStatusCode(responseBody, statusCode, 200L, 201L);

      // Get the _id from the body
      //
      JSONParser parser = new JSONParser();
      JSONObject bodyJs = (JSONObject) parser.parse(responseBody);
      String documentId = (String) bodyJs.get("_id");

      // Keep it around in the cache entry
      //
      cacheEntry.setInternalId(documentId);

      cacheEntry.setLastWritten(new Date());
    } catch (Exception e) {
      throw new HopException("Error writing caching file entry to OpenSearch", e);
    }
  }

  private String getAuthorizationHeaderValue(String actualUsername, String actualPassword) {
    String userPass = actualUsername + ':' + actualPassword;
    return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes());
  }

  @Override
  public void deleteCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      // We first search the document ID with the executionId.
      // Then we perform a DELETE /index-name/_doc/_id
      //
      // Search for the document with the given executionId
      //
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

      RestCaller restCaller =
          new RestCaller(
              metadataProvider,
              postUri.toString(),
              actualUsername,
              actualPassword,
              "POST",
              body,
              ignoreSsl,
              getHeaders());
      String responseBody = restCaller.execute();
      Long statusCode = restCaller.getStatusCode();

      // Verify the 201 from OpenSearch
      //
      checkStatusCode(responseBody, statusCode, 200L, 201L);

      // Get the _id from the response body
      //
      JSONParser parser = new JSONParser();
      JSONObject j = (JSONObject) parser.parse(responseBody);
      JSONObject jHitsTop = (JSONObject) j.get("hits");
      if (jHitsTop == null) {
        // Nothing left to do, something went wrong
        return;
      }
      JSONArray jHits = (JSONArray) jHitsTop.get("hits");
      if (jHits == null || jHits.isEmpty()) {
        // No hits returned
        return;
      }

      // Delete all the hits (in case some duplicates were introduced)
      //
      for (Object hit : jHits) {
        JSONObject jHit = (JSONObject) hit;
        String elasticId = (String) jHit.get("_id");
        URI deleteUri = uri.resolve(actualIndexName + "/_doc/" + elasticId);

        RestCaller deleteRestCaller =
            new RestCaller(
                metadataProvider,
                deleteUri.toString(),
                actualUsername,
                actualPassword,
                "DELETE",
                "",
                ignoreSsl,
                getHeaders());
        responseBody = deleteRestCaller.execute();
        checkStatusCode(responseBody, deleteRestCaller.getStatusCode(), 200L);
      }
    } catch (Exception e) {
      throw new HopException("Error deleting caching file entry from OpenSearch", e);
    } finally {
      cache.remove(cacheEntry.getId());
    }
  }

  private static void checkStatusCode(String responseBody, Long responseCode, Long... statusCodes)
      throws HopException {
    if (responseCode == null) {
      throw new HopException("No status code received with response body: " + responseBody);
    }
    boolean found = false;
    for (Long statusCode : statusCodes) {
      if (statusCode.equals(responseCode)) {
        found = true;
        break;
      }
    }
    if (!found) {
      throw new HopException(
          "Invalid status code received from OpenSearch: '"
              + responseCode
              + "' with response: "
              + responseBody);
    }
  }

  private Map<String, String> getHeaders() {
    return Map.of(
        "Content-Type",
        "application/json",
        "Accept",
        "application/json",
        "Authorization",
        getAuthorizationHeaderValue(actualUsername, actualPassword));
  }

  protected synchronized CacheEntry loadCacheEntry(String executionId) throws HopException {
    try {
      // Search for the document with the given executionId
      //
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
              .header("Authorization", getAuthorizationHeaderValue(actualUsername, actualPassword))
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();

      RestCaller restCaller =
          new RestCaller(
              metadataProvider,
              postUri.toString(),
              actualUsername,
              actualPassword,
              "POST",
              body,
              ignoreSsl,
              getHeaders());
      String responseBody = restCaller.execute();
      Long statusCode = restCaller.getStatusCode();

      checkStatusCode(responseBody, statusCode, 200L, 201L);

      // Now parse the returned JSON
      //
      JSONParser parser = new JSONParser();
      JSONObject j = (JSONObject) parser.parse(responseBody);
      JSONObject jHitsTop = (JSONObject) j.get("hits");
      if (jHitsTop == null) {
        // Nothing left to do, something went wrong
        return null;
      }
      JSONArray jHits = (JSONArray) jHitsTop.get("hits");
      if (jHits == null || jHits.isEmpty()) {
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
          "Error loading execution information location from OpenSearch using executionId '"
              + executionId
              + "'",
          e);
    }
  }

  @Override
  protected void retrieveIds(boolean includeChildren, Set<DatedId> ids, int limit)
      throws HopException {
    // Get all the IDs from OpenSearch if we don't have it in the cache.
    //
    try {
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

      RestCaller restCaller =
          new RestCaller(
              metadataProvider,
              postUri.toString(),
              actualUsername,
              actualPassword,
              "POST",
              body,
              ignoreSsl,
              getHeaders());
      String responseBody = restCaller.execute();
      Long statusCode = restCaller.getStatusCode();

      checkStatusCode(responseBody, statusCode, 200L, 201L);

      // Now parse the returned JSON
      //
      JSONParser parser = new JSONParser();
      JSONObject j = (JSONObject) parser.parse(responseBody);
      JSONObject jHitsTop = (JSONObject) j.get("hits");
      if (jHitsTop == null) {
        // Nothing left to do, something went wrong
        return;
      }
      JSONArray jHits = (JSONArray) jHitsTop.get("hits");
      if (jHits == null || jHits.isEmpty()) {
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
        if (jHitsFieldsIds == null || jHitsFieldsIds.isEmpty()) {
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
      throw new HopException("Error finding execution ids from OpenSearch", e);
    }
  }

  /** A button to create and configure the specified index */
  @GuiWidgetElement(
      id = "createIndexButton",
      order = "035",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::OpenSearchExecutionInfoLocation.CreateIndex.Label",
      toolTip = "i18n::OpenSearchExecutionInfoLocation.CreateIndex.Tooltip")
  public void createIndexButton(Object object) {
    HopGui hopGui = HopGui.getInstance();
    OpenSearchExecutionInfoLocation location = (OpenSearchExecutionInfoLocation) object;

    try {
      location.initialize(hopGui.getVariables(), hopGui.getMetadataProvider());
      String putBody =
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
      String result;
      RestCaller restCaller =
          new RestCaller(
              new MemoryMetadataProvider(),
              location.actualUrl + "/" + location.actualIndexName,
              location.actualUsername,
              location.actualPassword,
              "PUT",
              putBody,
              location.ignoreSsl,
              getHeaders());
      try {
        result = restCaller.execute();
      } catch (Exception e) {
        result = Const.getSimpleStackTrace(e);
      }
      if (restCaller.getResult() == null || restCaller.getResult().getNrErrors() > 0) {
        result += Const.CR + "Logging: " + restCaller.getLoggingText();
      }
      EnterTextDialog dialog =
          new EnterTextDialog(
              hopGui.getShell(),
              "Results",
              restCaller.getMethod() + " on " + restCaller.getUrl() + ":",
              "Code: "
                  + restCaller.getStatusCode()
                  + Const.CR
                  + Const.NVL(result, "Failed")
                  + Const.CR
                  + Const.CR
                  + "The result of the PUT on : "
                  + restCaller.getUrl()
                  + " with body: "
                  + Const.CR
                  + putBody,
              true);
      dialog.setReadOnly();
      dialog.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error creating OpenSearch index " + location.indexName, e);
    }
  }

  @Override
  public String getPluginId() {
    return "opensearch-location";
  }

  public void setPluginId(String pluginId) {
    // Don't set anything
  }

  @Override
  public String getPluginName() {
    return "OpenSearch location";
  }

  @Override
  public void setPluginName(String pluginName) {
    // Nothing to set
  }
}
