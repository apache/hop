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
import com.jayway.jsonpath.internal.path.PathCompiler;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.InputStream;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.RandomAccess;
import lombok.Getter;
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

  private ReadContext jsonReadContext;

  /** used if the incoming value is a String */
  @Getter private Configuration jsonConfiguration;

  /** used if the incoming value is a JsonNode */
  private final Configuration jsonNodeConfiguration;

  /**
   * Jayway refuses path functions under ALWAYS_RETURN_LIST, so function paths are read with these
   * copies of the configurations above that leave the option out.
   */
  private Configuration jsonFunctionConfiguration;

  private final Configuration jsonNodeFunctionConfiguration;

  /** The function configuration matching the current read context. */
  private Configuration functionConfiguration;

  private boolean ignoreMissingPath;
  @Getter private boolean defaultPathLeafToNull;

  private JsonInputField[] fields;
  private JsonPath[] paths = null;

  /** Per path: it ends in a function such as length(). */
  private boolean[] functionPaths = new boolean[0];

  /** Per path: it matches at most one element, so a function on it yields one value. */
  private boolean[] definitePaths = new boolean[0];

  private final ILogChannel log;

  private static final Option[] DEFAULT_OPTIONS = {
    Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
  };

  protected FastJsonReader(ILogChannel log) {
    this.ignoreMissingPath = false;
    this.defaultPathLeafToNull = true;
    this.jsonConfiguration = Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);
    this.jsonNodeConfiguration = getJacksonNodeJsonPathConfig();
    this.jsonFunctionConfiguration = withoutAlwaysReturnList(this.jsonConfiguration);
    this.jsonNodeFunctionConfiguration = withoutAlwaysReturnList(this.jsonNodeConfiguration);
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
        this.jsonFunctionConfiguration = withoutAlwaysReturnList(this.jsonConfiguration);
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

  private static Configuration withoutAlwaysReturnList(Configuration config) {
    EnumSet<Option> options = EnumSet.noneOf(Option.class);
    options.addAll(config.getOptions());
    options.remove(Option.ALWAYS_RETURN_LIST);
    return config.setOptions(options.toArray(new Option[0]));
  }

  @SuppressWarnings("javabugs:S2259") // the configuration is created before it is logged
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
    jsonReadContext = getParseContext().parse(is, Const.UTF_8);
    functionConfiguration = jsonFunctionConfiguration;
    if (jsonReadContext == null) {
      throw new HopException(BaseMessages.getString(PKG, "JsonReader.Error.ReadUrl.Null"));
    }
  }

  protected void readInput(JsonNode node) throws HopException {
    jsonReadContext = getJsonNodeParseContext().parse(node);
    functionConfiguration = jsonNodeFunctionConfiguration;
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
    this.functionPaths = new boolean[paths.length];
    this.definitePaths = new boolean[paths.length];
    for (int i = 0; i < paths.length; i++) {
      // JsonPath does not expose whether a path ends in a function; its compiler does.
      functionPaths[i] = PathCompiler.compile(fields[i].getPath()).isFunctionPath();
      definitePaths[i] = paths[i].isDefinite();
    }
  }

  @Override
  public IRowSet emptyFieldRowSet() {
    return getEmptyResponse();
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
    private final List<List<?>> results;
    private final int rowCount;
    private int rowNbr;

    private final boolean includeNulls =
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
      /*
       * if should skip null-only rows; size won't be exact if set. If HOP_JSON_INPUT_INCLUDE_NULLS is
       * "Y" (default behavior) then nulls will be included otherwise they will not
       */
      boolean allNulls = rowCount > 1;
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
    for (int i = 0; i < paths.length; i++) {
      List<Object> result =
          functionPaths[i]
              ? readFunctionPath(i)
              : normalizeJsonPathResult(getReadContext().read(paths[i]));
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
    }
    return results;
  }

  /**
   * Evaluate a path that ends in a function. A definite path gives one value, even when that value
   * is itself an array (keys(), first() of a nested array). A function behind a wildcard, such as
   * {@code $.items[*].name.length()}, is applied to each match and gives one value per match. A
   * missing value is an empty result, like a regular path that matches nothing.
   */
  private List<Object> readFunctionPath(int index) throws JsonInputException {
    Object document = getReadContext().json();
    Object value;
    try {
      value = paths[index].read(document, functionConfiguration);
    } catch (RuntimeException e) {
      // SUPPRESS_EXCEPTIONS does not cover errors raised by the function itself, such as sum() or
      // first() on an empty array. Treat them as a missing value, or report the actual cause.
      if (!isIgnoreMissingPath()) {
        throw new JsonInputException(
            BaseMessages.getString(
                PKG, "JsonReader.Error.PathFunction", fields[index].getPath(), e.getMessage()),
            e);
      }
      if (log.isDebug()) {
        log.logDebug(
            BaseMessages.getString(
                PKG, "JsonReader.Error.PathFunction", fields[index].getPath(), e.getMessage()));
      }
      return Collections.emptyList();
    }
    if (value == null) {
      return Collections.emptyList();
    }
    if (definitePaths[index]) {
      return Collections.singletonList(value);
    }
    return normalizeJsonPathResult(value);
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
