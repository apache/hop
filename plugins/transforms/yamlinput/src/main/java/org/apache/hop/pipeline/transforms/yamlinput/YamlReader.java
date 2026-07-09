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

package org.apache.hop.pipeline.transforms.yamlinput;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.yaml.snakeyaml.Yaml;

/**
 * Read YAML files, parse them and convert them to rows and writes these to one or more output
 * streams.
 */
public class YamlReader {
  private static final String DEFAULT_LIST_VALUE_NAME = "Value";

  private static final Configuration JSON_PATH_CONFIG =
      Configuration.defaultConfiguration()
          .addOptions(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL);

  private String string;
  @Getter private FileObject file;

  // document
  // Store all documents available
  private List<Object> documents;
  // Store current document
  @Getter private Object document;
  // Store document iterator
  private Iterator<Object> documentIt;

  // object current inside a document
  // In case we use a list
  private Object dataList;
  // Store object iterator
  private Iterator<Object> dataListIt;
  private boolean useMap;
  @Getter private Yaml yaml;

  /** yaml field path */
  private String[] fieldPaths;

  private JsonPath[] compiledPaths;
  private String[] discoveredFieldPaths;

  private final Deque<Object[]> pendingRows = new ArrayDeque<>();

  /** Bytes read from the last loaded file (for data volume tracking). */
  @Getter private long bytesReadFromFile;

  public YamlReader() {
    this.string = null;
    this.file = null;
    this.documents = new ArrayList<>();
    this.useMap = true;
    this.dataList = null;
    this.yaml = new Yaml();
  }

  /**
   * Sets the YAML field paths to be extracted and pre-compiles them into {@link JsonPath} instances
   * for better runtime performance.
   *
   * @param paths the array of field paths to extract; {@code null} clears all configured paths
   */
  public void setFieldPaths(String[] paths) {
    if (paths == null) {
      this.fieldPaths = null;
      this.compiledPaths = null;
      return;
    }

    this.fieldPaths = paths.clone();
    this.compiledPaths = new JsonPath[paths.length];
    for (int i = 0; i < paths.length; i++) {
      this.compiledPaths[i] = JsonPath.compile(YamlPathResolver.normalize(paths[i]));
    }
  }

  public void loadFile(FileObject file) throws Exception {
    this.file = file;

    try (CountingInputStream is = new CountingInputStream(HopVfs.getInputStream(getFile()))) {
      for (Object data : getYaml().loadAll(is)) {
        documents.add(data);
        this.useMap = (data instanceof Map);
      }
      bytesReadFromFile = is.getCount();
      this.documentIt = documents.iterator();
    }
  }

  public void loadString(String string) {
    this.string = string;
    bytesReadFromFile = 0;
    for (Object data : getYaml().loadAll(getStringValue())) {
      documents.add(data);
      this.useMap = (data instanceof Map);
    }
    this.documentIt = documents.iterator();
  }

  public boolean isMapUsed() {
    return this.useMap;
  }

  /** get row */
  public Object[] getRow(IRowMeta rowMeta) {
    if (!pendingRows.isEmpty()) {
      Object[] row = pendingRows.poll();
      if (pendingRows.isEmpty()) {
        finishDocument();
      }
      return row;
    }

    if (document == null) {
      getNextDocument();
    }

    if (document == null) {
      return new Object[0];
    }

    Object[] row = fetchRow(rowMeta);
    if (row.length == 0 && !isFinishedDocument()) {
      return getRow(rowMeta);
    }
    return row;
  }

  /**
   * Ensures that the configured field paths have been compiled into {@link JsonPath} instances.
   *
   * @param rowMeta the row metadata used to determine field paths when they have not been
   *     explicitly configured
   */
  private void ensurePathsCompiled(IRowMeta rowMeta) {
    if (compiledPaths != null && compiledPaths.length == rowMeta.size()) {
      return;
    }

    String[] paths = new String[rowMeta.size()];
    if (discoveredFieldPaths != null && discoveredFieldPaths.length == rowMeta.size()) {
      System.arraycopy(discoveredFieldPaths, 0, paths, 0, paths.length);
    } else {
      for (int i = 0; i < rowMeta.size(); i++) {
        paths[i] = rowMeta.getValueMeta(i).getName();
      }
    }
    setFieldPaths(paths);
  }

  private Object[] fetchRow(IRowMeta rowMeta) {
    ensurePathsCompiled(rowMeta);

    if (isMapUsed()) {
      return processMapRow(rowMeta, document, false);
    }

    return processListRow(rowMeta);
  }

  private Object[] handleCurrentListValue(IRowMeta rowMeta) {
    if (dataList instanceof Map<?, ?>) {
      return processMapRow(rowMeta, dataList, true);
    }

    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    return new Object[] {getValue(dataList, valueMeta)};
  }

  private Object[] processListRow(IRowMeta rowMeta) {
    if (dataList == null) {
      return advanceListIterator();
    }

    Object[] row = handleCurrentListValue(rowMeta);
    dataList = null;
    return row;
  }

  private Object[] processMapRow(IRowMeta rowMeta, Object root, boolean listElement) {
    if (pendingRows.isEmpty()) {
      prepareRowsFromPaths(rowMeta, root, listElement);
    }

    Object[] row = pendingRows.poll();
    if (pendingRows.isEmpty()) {
      finishDocument();
    }
    return row == null ? new Object[0] : row;
  }

  private void prepareRowsFromPaths(IRowMeta rowMeta, Object root, boolean listElement) {
    List<List<Object>> columnValues = new ArrayList<>(compiledPaths.length);
    int rowCount = 1;

    for (int pathIndex = 0; pathIndex < compiledPaths.length; pathIndex++) {
      JsonPath path = pathForRoot(compiledPaths[pathIndex], fieldPaths[pathIndex], listElement);
      List<Object> values = readPathAsList(root, path);
      if (values.size() > rowCount) {
        rowCount = values.size();
      }
      columnValues.add(values);
    }

    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      Object[] row = new Object[rowMeta.size()];
      for (int col = 0; col < rowMeta.size(); col++) {
        List<Object> values = columnValues.get(col);
        Object raw = pickValue(values, rowIndex, rowCount);
        row[col] = getValue(raw, rowMeta.getValueMeta(col));
      }
      pendingRows.add(row);
    }
  }

  private static JsonPath pathForRoot(
      JsonPath compiledPath, String originalPath, boolean listElement) {
    if (!listElement || originalPath == null || !originalPath.contains("[*]")) {
      return compiledPath;
    }

    String adapted = originalPath.replaceFirst("\\$\\[\\*]", "\\$");
    return JsonPath.compile(YamlPathResolver.normalize(adapted));
  }

  private static Object pickValue(List<Object> values, int rowIndex, int rowCount) {
    if (values == null || values.isEmpty()) {
      return null;
    }

    if (values.size() == 1 && rowCount > 1) {
      return values.getFirst();
    }
    if (rowIndex < values.size()) {
      return values.get(rowIndex);
    }
    return null;
  }

  private List<Object> readPathAsList(Object root, JsonPath path) {
    if (root == null) {
      return List.of((Object) null);
    }

    try {
      ReadContext context = JsonPath.using(JSON_PATH_CONFIG).parse(root);
      Object result = context.read(path);
      return toValueList(result);
    } catch (Exception e) {
      return List.of((Object) null);
    }
  }

  private static List<Object> toValueList(Object result) {
    if (result == null) {
      return List.of((Object) null);
    }

    if (result instanceof List<?> list) {
      if (list.isEmpty()) {
        return List.of((Object) null);
      }
      return new ArrayList<>(list);
    }
    return List.of(result);
  }

  private Object[] advanceListIterator() {
    if (!dataListIt.hasNext()) {
      finishDocument();
      return new Object[0];
    }

    dataList = dataListIt.next();
    return new Object[0];
  }

  private void getNextDocument() {
    pendingRows.clear();
    // See if we have another document
    if (this.documentIt.hasNext()) {
      // We have another document
      this.document = this.documentIt.next();
      if (!isMapUsed()) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) getDocument();
        dataListIt = list.iterator();
      }
    }
  }

  private String setMap(Object value) {
    StringBuilder result = new StringBuilder(value.toString());
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      Iterator<Map.Entry<Object, Object>> it = map.entrySet().iterator();

      int nr = 0;
      while (it.hasNext()) {
        Map.Entry<Object, Object> pairs = it.next();
        String res = pairs.getKey().toString() + ":  " + setMap(pairs.getValue());
        if (nr == 0) {
          result = new StringBuilder("{" + res);
        } else {
          result.append(",").append(res);
        }
        nr++;
      }
      if (nr > 0) {
        result.append("}");
      }
    }
    return result.toString();
  }

  private int getType(Object value) {
    if (value == null) {
      return IValueMeta.TYPE_STRING;
    }

    return switch (value) {
      case Integer ignored -> IValueMeta.TYPE_INTEGER;
      case Long ignored -> IValueMeta.TYPE_INTEGER;
      case Double ignored -> IValueMeta.TYPE_NUMBER;
      case Boolean ignored -> IValueMeta.TYPE_BOOLEAN;
      case BigInteger ignored -> IValueMeta.TYPE_BIGNUMBER;
      case BigDecimal ignored -> IValueMeta.TYPE_BIGNUMBER;
      case Byte ignored -> IValueMeta.TYPE_BINARY;
      case Timestamp ignored -> IValueMeta.TYPE_DATE;
      case java.sql.Date ignored -> IValueMeta.TYPE_DATE;
      case java.util.Date ignored -> IValueMeta.TYPE_DATE;
      default -> IValueMeta.TYPE_STRING;
    };
  }

  private void finishDocument() {
    this.document = null;
    pendingRows.clear();
  }

  private boolean isFinishedDocument() {
    return (this.document == null);
  }

  public void close() throws IOException {
    if (file != null) {
      file.close();
    }
    this.documents = null;
    this.yaml = null;
  }

  public String getStringValue() {
    return this.string;
  }

  /** get value */
  private Object getValue(Object value, IValueMeta valueMeta) {
    if (value == null) {
      return null;
    }

    Object normalizedValue = normalizeList(value);

    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_INTEGER -> convertToInteger(normalizedValue);
      case IValueMeta.TYPE_NUMBER -> convertToDouble(normalizedValue);
      case IValueMeta.TYPE_BIGNUMBER -> convertToBigDecimal(normalizedValue);
      case IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_DATE, IValueMeta.TYPE_BINARY -> normalizedValue;
      default -> convertToString(normalizedValue, valueMeta);
    };
  }

  private Object normalizeList(Object value) {
    if (value instanceof List) {
      String dumped = getYaml().dump(value);
      if (StringUtils.isEmpty(dumped)) {
        return dumped;
      }

      return stripTrailingLineBreaks(dumped);
    }
    return value;
  }

  /** SnakeYAML {@code dump} ends the stream with a line break; strip it for cell values. */
  private static String stripTrailingLineBreaks(String s) {
    int end = s.length();
    while (end > 0) {
      char c = s.charAt(end - 1);
      if (c != '\n' && c != '\r') {
        break;
      }
      end--;
    }
    return s.substring(0, end);
  }

  /** object -> long */
  private Long convertToInteger(Object value) {
    return switch (value) {
      case Integer i -> i.longValue();
      case BigInteger bi -> bi.longValue();
      case Long l -> l;
      default -> Long.valueOf(value.toString());
    };
  }

  /** object -> double */
  private Double convertToDouble(Object value) {
    return switch (value) {
      case Integer i -> i.doubleValue();
      case BigInteger bi -> bi.doubleValue();
      case Long l -> l.doubleValue();
      case Double d -> d;
      default -> Double.valueOf(value.toString());
    };
  }

  private BigDecimal convertToBigDecimal(Object value) {
    return switch (value) {
      case Integer i -> new BigDecimal(i);
      case BigInteger bi -> new BigDecimal(bi);
      case Long l -> BigDecimal.valueOf(l);
      case Double d -> BigDecimal.valueOf(d);
      default -> null;
    };
  }

  private String convertToString(Object value, IValueMeta valueMeta) {
    String s = setMap(value);
    return applyTrim(s, valueMeta);
  }

  private String applyTrim(String s, IValueMeta valueMeta) {
    return switch (valueMeta.getTrimType()) {
      case YamlInputField.TYPE_TRIM_LEFT -> Const.ltrim(s);
      case YamlInputField.TYPE_TRIM_RIGHT -> Const.rtrim(s);
      case YamlInputField.TYPE_TRIM_BOTH -> Const.trim(s);
      default -> s;
    };
  }

  /** get fields. */
  public RowMeta getFields() {
    RowMeta rowMeta = new RowMeta();
    List<String> paths = getDiscoveredPaths();
    discoveredFieldPaths = paths.toArray(new String[0]);

    for (String path : paths) {
      Object sample = null;
      for (Object data : documents) {
        sample = readSampleValue(data, path);
        if (sample != null) {
          break;
        }
      }
      addField(rowMeta, path, sample);
    }

    return rowMeta;
  }

  /** Returns JsonPath expressions discovered in the loaded YAML documents. */
  public List<String> getDiscoveredPaths() {
    Set<String> paths = new LinkedHashSet<>();
    for (Object data : documents) {
      collectPaths(data, YamlPathResolver.DOLLAR, paths);
    }
    return new ArrayList<>(paths);
  }

  private Object readSampleValue(Object root, String path) {
    if (root == null) {
      return null;
    }
    List<Object> values = readPathAsList(root, JsonPath.compile(path));
    return values.isEmpty() ? null : values.getFirst();
  }

  private void collectPaths(Object node, String prefix, Set<String> paths) {
    if (node instanceof Map<?, ?> map) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = String.valueOf(entry.getKey());
        String segment = formatPathSegment(key);
        String childPath =
            YamlPathResolver.DOLLAR.equals(prefix)
                ? prefix + "." + segment
                : prefix + "." + segment;
        Object value = entry.getValue();
        if (value instanceof Map || value instanceof List) {
          collectPaths(value, childPath, paths);
        } else {
          paths.add(childPath);
        }
      }
    } else if (node instanceof List<?> list) {
      if (!list.isEmpty()) {
        Object first = list.getFirst();
        if (first instanceof Map || first instanceof List) {
          collectPaths(first, prefix + "[*]", paths);
        } else {
          paths.add(prefix + "[*]");
        }
      }
    } else if (YamlPathResolver.DOLLAR.equals(prefix)) {
      paths.add(YamlPathResolver.DOLLAR);
    }
  }

  private static String formatPathSegment(String key) {
    if (key.contains(".") || key.contains(" ") || key.contains("[")) {
      return "['" + key.replace("'", "\\'") + "']";
    }

    return key;
  }

  private void addField(RowMeta rowMeta, String path, Object value) {
    String fieldName = fieldNameFromPath(path);

    IValueMeta valueMeta;
    try {
      valueMeta = ValueMetaFactory.createValueMeta(fieldName, getType(value));
    } catch (HopPluginException e) {
      valueMeta = new ValueMetaNone(fieldName);
    }

    rowMeta.addValueMeta(valueMeta);
  }

  static String fieldNameFromPath(String path) {
    if (Utils.isEmpty(path) || YamlPathResolver.DOLLAR.equals(path)) {
      return path;
    }
    if ("$[*]".equals(path)) {
      return DEFAULT_LIST_VALUE_NAME;
    }

    int bracket = path.lastIndexOf("['");
    if (bracket >= 0) {
      int quoteEnd = path.indexOf("']", bracket);
      if (quoteEnd > bracket) {
        return path.substring(bracket + 2, quoteEnd).replace("\\'", "'");
      }
    }

    int dot = path.lastIndexOf('.');
    if (dot >= 0 && dot < path.length() - 1) {
      String leaf = path.substring(dot + 1);
      int open = leaf.indexOf('[');
      return open >= 0 ? leaf.substring(0, open) : leaf;
    }

    if (path.startsWith(YamlPathResolver.DOLLAR)) {
      return path.substring(1);
    }
    return path;
  }
}
