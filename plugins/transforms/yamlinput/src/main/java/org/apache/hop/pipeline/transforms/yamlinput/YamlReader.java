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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private Object[] fetchRow(IRowMeta rowMeta) {
    if (isMapUsed()) {
      return processMapRow(rowMeta, (Map<?, ?>) document);
    }

    return processListRow(rowMeta);
  }

  private Object[] handleCurrentListValue(IRowMeta rowMeta) {
    List<?> list = (List<?>) document;

    if (list.size() == 1 && list.getFirst() instanceof Map<?, ?> map) {
      return processMapRow(rowMeta, map);
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

  private Object[] processMapRow(IRowMeta rowMeta, Map<?, ?> map) {
    Object[] row = new Object[rowMeta.size()];

    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);

      Object value =
          Utils.isEmpty(valueMeta.getName()) ? document.toString() : map.get(valueMeta.getName());

      row[i] = getValue(value, valueMeta);
    }

    finishDocument();
    return row;
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

    for (Object data : documents) {
      if (data instanceof Map<?, ?> map) {
        appendFromMap(rowMeta, map);
      } else if (data instanceof List<?> list) {
        appendFromList(rowMeta, list);
      }
    }

    return rowMeta;
  }

  private void appendFromList(RowMeta rowMeta, List<?> list) {
    if (list.isEmpty()) {
      return;
    }

    Object first = list.getFirst();
    if (list.size() == 1 && first instanceof Map<?, ?> map) {
      appendFromMap(rowMeta, map);
    } else {
      addField(rowMeta, DEFAULT_LIST_VALUE_NAME, first);
    }
  }

  private void appendFromMap(RowMeta rowMeta, Map<?, ?> map) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      addField(rowMeta, entry.getKey(), entry.getValue());
    }
  }

  private void addField(RowMeta rowMeta, Object key, Object value) {
    String fieldName = String.valueOf(key);

    IValueMeta valueMeta;
    try {
      valueMeta = ValueMetaFactory.createValueMeta(fieldName, getType(value));
    } catch (HopPluginException e) {
      valueMeta = new ValueMetaNone(fieldName);
    }

    rowMeta.addValueMeta(valueMeta);
  }
}
