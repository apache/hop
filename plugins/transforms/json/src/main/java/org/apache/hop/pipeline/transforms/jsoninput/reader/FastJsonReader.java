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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.InputStream;
import java.util.*;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputMeta;
import org.apache.hop.pipeline.transforms.jsoninput.exception.JsonInputException;

public class FastJsonReader implements IJsonReader {
  private static final Class<?> PKG = JsonInputMeta.class;

  // as per RFC 7159, the default JSON encoding shall be UTF-8
  // see https://tools.ietf.org/html/rfc7159#section-8.1
  private static final String JSON_CHARSET = "UTF-8";

  private ReadContext jsonReadContext;

  /** used if the incoming value is a String */
  private Configuration jsonConfiguration;

  /** used if the incoming value is a JsonNode */
  private Configuration jsonNodeConfiguration;

  private boolean ignoreMissingPath;
  private boolean defaultPathLeafToNull;

  private JsonInputField[] fields;
  private JsonPath[] paths = null;
  private ILogChannel log;

  private static final Option[] DEFAULT_OPTIONS = {
    Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
  };

  protected FastJsonReader(ILogChannel log) throws HopException {
    this.ignoreMissingPath = false;
    this.defaultPathLeafToNull = true;
    this.jsonConfiguration = Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);
    this.jsonNodeConfiguration = getJacksonNodeJsonPathConfig();
    this.log = log;
  }

  public FastJsonReader(JsonInputField[] fields, ILogChannel log) throws HopException {
    this(log);
    setFields(fields);
  }

  public FastJsonReader(JsonInputField[] fields, boolean defaultPathLeafToNull, ILogChannel log)
      throws HopException {
    this(fields, log);
    setDefaultPathLeafToNull(defaultPathLeafToNull);
  }

  private void setDefaultPathLeafToNull(boolean value) {
    if (value != this.defaultPathLeafToNull) {
      this.defaultPathLeafToNull = value;
      if (!this.defaultPathLeafToNull) {
        this.jsonConfiguration =
            deleteOptionFromConfiguration(this.jsonConfiguration, Option.DEFAULT_PATH_LEAF_TO_NULL);
      }
    }
  }

  private Configuration getJacksonNodeJsonPathConfig() {
    return Configuration.builder()
        .jsonProvider(new JacksonJsonNodeJsonProvider())
        .mappingProvider(new JacksonMappingProvider())
        .options(DEFAULT_OPTIONS)
        .build();
  }

  public boolean isDefaultPathLeafToNull() {
    return defaultPathLeafToNull;
  }

  private Configuration deleteOptionFromConfiguration(Configuration config, Option option) {
    Configuration currentConf = config;
    if (currentConf != null) {
      EnumSet<Option> currentOptions = EnumSet.noneOf(Option.class);
      currentOptions.addAll(currentConf.getOptions());
      if (currentOptions.remove(option)) {
        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(PKG, "JsonReader.Debug.Configuration.Option.Delete", option));
        }
        currentConf =
            Configuration.defaultConfiguration()
                .addOptions(currentOptions.toArray(new Option[currentOptions.size()]));
      }
    }
    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG, "JsonReader.Debug.Configuration.Options", currentConf.getOptions()));
    }
    return currentConf;
  }

  Configuration getJsonConfiguration() {
    return jsonConfiguration;
  }

  @Override
  public void setIgnoreMissingPath(boolean value) {
    this.ignoreMissingPath = value;
  }

  // used if incoming value is String
  private ParseContext getParseContext() {
    return JsonPath.using(jsonConfiguration);
  }

  // used if incoming value is JsonNode
  private ParseContext getJsonNodeParseContext() {
    return JsonPath.using(jsonNodeConfiguration);
  }

  private ReadContext getReadContext() {
    return jsonReadContext;
  }

  private static JsonPath[] compilePaths(JsonInputField[] fields) throws HopException {
    JsonPath[] paths = new JsonPath[fields.length];
    int i = 0;
    try {
      for (JsonInputField field : fields) {
        paths[i++] = JsonPath.compile(field.getPath());
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "JsonParser.JsonPath.Compile.Error", e.getMessage()));
    }
    return paths;
  }

  protected void readInput(InputStream is) throws HopException {
    jsonReadContext = getParseContext().parse(is, JSON_CHARSET);
    if (jsonReadContext == null) {
      throw new HopException(BaseMessages.getString(PKG, "JsonReader.Error.ReadUrl.Null"));
    }
  }

  protected void readInput(JsonNode node) throws HopException {
    jsonReadContext = getJsonNodeParseContext().parse(node);
    if (jsonReadContext == null) {
      throw new HopException(BaseMessages.getString(PKG, "JsonReader.Error.ReadUrl.Null"));
    }
  }

  @Override
  public boolean isIgnoreMissingPath() {
    return this.ignoreMissingPath;
  }

  @Override
  public void setFields(JsonInputField[] fields) throws HopException {
    this.fields = fields;
    this.paths = compilePaths(fields);
  }

  @Override
  public IRowSet parseStringValue(InputStream in) throws HopException {
    readInput(in);
    return getRow();
  }

  @Override
  public IRowSet parseJsonNodeValue(JsonNode node) throws HopException {
    readInput(node);
    return getRow();
  }

  private IRowSet getRow() throws HopException {
    List<List<?>> results = evalCombinedResult();
    int len = results.isEmpty() ? 0 : getMaxRowSize(results);
    if (log.isDetailed()) {
      log.logDetailed(BaseMessages.getString(PKG, "JsonInput.Log.NrRecords", len));
    }
    if (len == 0) {
      return getEmptyResponse();
    }
    return new TransposedRowSet(results);
  }

  /**
   * Gets the max size of the result rows.
   *
   * @param results A list of lists representing the result rows
   * @return the size of the largest row in the results
   */
  protected static int getMaxRowSize(List<List<?>> results) {
    return results.stream().mapToInt(List::size).max().getAsInt();
  }

  private IRowSet getEmptyResponse() {
    IRowSet nullInputResponse = new SingleRowRowSet();
    nullInputResponse.putRow(null, new Object[fields.length]);
    nullInputResponse.setDone();
    return nullInputResponse;
  }

  private static class TransposedRowSet extends SingleRowRowSet {
    private List<List<?>> results;
    private final int rowCount;
    private int rowNbr;

    /**
     * if should skip null-only rows; size won't be exact if set. If HOP_JSON_INPUT_INCLUDE_NULLS is
     * "Y" (default behavior) then nulls will be included otherwise they will not
     */
    private boolean cullNulls = true;

    private boolean includeNulls =
        "Y"
            .equalsIgnoreCase(
                System.getProperty(
                    Const.HOP_JSON_INPUT_INCLUDE_NULLS, Const.JSON_INPUT_INCLUDE_NULLS));

    public TransposedRowSet(List<List<?>> results) {
      super();
      this.results = results;
      this.rowCount = results.isEmpty() ? 0 : FastJsonReader.getMaxRowSize(results);
    }

    @Override
    public Object[] getRow() {
      boolean allNulls = cullNulls && rowCount > 1;
      Object[] rowData = null;
      do {
        if (rowNbr >= rowCount) {
          results.clear();
          return null;
        }
        rowData = new Object[results.size()];
        for (int col = 0; col < results.size(); col++) {
          if (results.get(col).isEmpty()) {
            rowData[col] = null;
            continue;
          }
          Object val = results.get(col).get(rowNbr);
          rowData[col] = val;
          allNulls &= (val == null && !includeNulls);
        }
        rowNbr++;
      } while (allNulls);
      return rowData;
    }

    @Override
    public int size() {
      return rowCount - rowNbr;
    }

    @Override
    public boolean isDone() {
      // built at ctor
      return true;
    }

    @Override
    public void clear() {
      results.clear();
    }
  }

  private List<List<?>> evalCombinedResult() throws JsonInputException {
    int lastSize = -1;
    String prevPath = null;
    List<List<?>> results = new ArrayList<>(paths.length);
    int i = 0;
    for (JsonPath path : paths) {
      Object raw = getReadContext().read(path);
      List<Object> result = normalizeJsonPathResult(raw);
      if (result.size() != lastSize && lastSize > 0 && !result.isEmpty()) {
        throw new JsonInputException(
            BaseMessages.getString(
                PKG,
                "JsonInput.Error.BadStructure",
                result.size(),
                fields[i].getPath(),
                prevPath,
                lastSize));
      }
      if (!isIgnoreMissingPath() && (isAllNull(result) || result.isEmpty())) {
        throw new JsonInputException(
            BaseMessages.getString(PKG, "JsonReader.Error.CanNotFindPath", fields[i].getPath()));
      }
      results.add(result);
      lastSize = result.size();
      prevPath = fields[i].getPath();
      i++;
    }
    return results;
  }

  public static boolean isAllNull(Iterable<?> list) {
    for (Object obj : list) {
      if (obj != null) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> normalizeJsonPathResult(Object r) throws JsonInputException {
    if (r instanceof List<?>) {
      // Already a List
      return (List<Object>) r;
    }
    if (r instanceof ArrayNode arr) {
      // expose array elements as a List view,
      // doesn't do conversion for performance
      return new ArrayNodeListView(arr);
    }

    throw new JsonInputException(
        "Unexpected JsonPath result type: "
            + r.getClass().getName()
            + ". Expected List<?> or ArrayNode.");
  }

  /** A List view over an ArrayNode's elements to use its nodes without doing conversion. */
  private static final class ArrayNodeListView extends AbstractList<Object>
      implements RandomAccess {
    private final ArrayNode arr;

    ArrayNodeListView(ArrayNode arr) {
      this.arr = arr;
    }

    @Override
    public Object get(int index) {
      return arr.get(index);
    } // returns JsonNode

    @Override
    public int size() {
      return arr.size();
    }
  }
}
