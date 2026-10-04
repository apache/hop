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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.maskfields.MaskingEngine.Binding;
import org.apache.hop.pipeline.transforms.maskfields.store.DatabaseMaskingStore;
import org.apache.hop.pipeline.transforms.maskfields.store.IMaskingStore;
import org.apache.hop.pipeline.transforms.maskfields.store.MemoryMaskingStore;

/** Replaces field values using masking patterns. */
public class MaskFields extends BaseTransform<MaskFieldsMeta, MaskFieldsData> {

  private static final Class<?> PKG = MaskFields.class;

  public MaskFields(
      TransformMeta transformMeta,
      MaskFieldsMeta meta,
      MaskFieldsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    TransformMeta transform = getTransformMeta();
    if (transform != null && (transform.getCopies(this) > 1 || transform.isPartitioned())) {
      logError(BaseMessages.getString(PKG, "MaskFields.Error.Copies"));
      return false;
    }
    Map<String, IMaskingStore> stores = new LinkedHashMap<>();
    try {
      List<Binding> bindings = new ArrayList<>();
      Map<String, MaskingPattern> patterns = new LinkedHashMap<>();
      if (meta.getFields() != null) {
        for (MaskField field : meta.getFields()) {
          if (field == null || StringUtils.isEmpty(field.getFieldName())) {
            continue;
          }
          if (StringUtils.isEmpty(field.getPatternName())) {
            throw new HopException(
                BaseMessages.getString(PKG, "MaskFields.Check.NoPattern", field.getFieldName()));
          }
          MaskingPattern pattern = patterns.get(field.getPatternName());
          if (pattern == null) {
            pattern = loadRequired(field.getPatternName());
            patterns.put(field.getPatternName(), pattern);
          }
          long start = parseStart(pattern);
          IMaskingStore store = storeFor(pattern, stores);
          bindings.add(
              new Binding(
                  field.getFieldName(),
                  pattern,
                  resolve(pattern.getPrefix()),
                  resolve(pattern.getSuffix()),
                  start,
                  store));
        }
      }
      data.engine = new MaskingEngine(bindings, new ArrayList<>(stores.values()));
      return true;
    } catch (HopException e) {
      closeStores(stores);
      logError(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) {
      first = false;
      readLists();
    }
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    if (data.outputRowMeta == null) {
      data.outputRowMeta = getInputRowMeta().clone();
      validateTypes(data.outputRowMeta);
    }
    try {
      data.engine.apply(data.outputRowMeta, row);
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(data.outputRowMeta, row, 1L, e.getMessage(), null, "MASK001");
        return true;
      }
      throw e;
    }
    putRow(data.outputRowMeta, row);
    return true;
  }

  private void validateTypes(IRowMeta rowMeta) throws HopException {
    for (Binding binding : data.engine.getBindings()) {
      int index = rowMeta.indexOfValue(binding.fieldName);
      if (index >= 0) {
        IValueMeta valueMeta = rowMeta.getValueMeta(index);
        String problem = MaskingRules.incompatibility(valueMeta, binding.pattern);
        if (problem != null) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "MaskFields.Check.Incompatible",
                  binding.fieldName,
                  binding.pattern.getName(),
                  problem));
        }
      }
    }
  }

  @Override
  public void dispose() {
    if (data.engine != null) {
      data.engine.close();
      data.engine = null;
    }
    super.dispose();
  }

  private void readLists() throws HopException {
    List<Binding> lists = new ArrayList<>();
    for (Binding binding : data.engine.getBindings()) {
      if (binding.pattern.getValueSource() == MaskingValueSource.LIST) {
        lists.add(binding);
      }
    }
    if (lists.isEmpty()) {
      return;
    }
    if (StringUtils.isEmpty(meta.getInfoTransformName())) {
      throw new HopException(BaseMessages.getString(PKG, "MaskFields.Check.NoInfo"));
    }
    IRowSet rowSet = findInputRowSet(meta.getInfoTransformName());
    if (rowSet == null) {
      throw new HopException(BaseMessages.getString(PKG, "MaskFields.Check.NoInfo"));
    }
    Object[] infoRow;
    while ((infoRow = getRowFrom(rowSet)) != null) {
      IRowMeta infoMeta = rowSet.getRowMeta();
      for (Binding binding : lists) {
        String listField = resolve(binding.pattern.getListField());
        int index = infoMeta.indexOfValue(listField);
        if (index < 0) {
          throw new HopException(
              BaseMessages.getString(PKG, "MaskFields.Error.ListFieldMissing", listField));
        }
        String text = infoMeta.getValueMeta(index).getString(infoRow[index]);
        if (StringUtils.isNotEmpty(text)) {
          binding.listValues.add(text);
        }
      }
    }
    for (Binding binding : lists) {
      if (binding.listValues.isEmpty()) {
        throw new HopException(
            BaseMessages.getString(PKG, "MaskFields.Error.EmptyList", binding.pattern.getName()));
      }
    }
  }

  private MaskingPattern loadRequired(String name) throws HopException {
    MaskingPattern pattern;
    try {
      pattern = meta.loadPattern(getMetadataProvider(), name);
    } catch (HopException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Check.MissingPattern", name), e);
    }
    if (pattern == null) {
      throw new HopException(BaseMessages.getString(PKG, "MaskFields.Check.MissingPattern", name));
    }
    if (StringUtils.isEmpty(pattern.getName())) {
      pattern.setName(name);
    }
    return pattern;
  }

  private long parseStart(MaskingPattern pattern) throws HopException {
    String text = resolve(pattern.getSequenceStart());
    if (StringUtils.isBlank(text)) {
      return 1L;
    }
    try {
      return Long.parseLong(text.trim());
    } catch (NumberFormatException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Error.BadSequence", text, pattern.getName()));
    }
  }

  private IMaskingStore storeFor(MaskingPattern pattern, Map<String, IMaskingStore> stores)
      throws HopException {
    if (!pattern.remembers()
        || pattern.getValueSource() == MaskingValueSource.SET_NULL
        || pattern.getValueSource() == MaskingValueSource.SET_EMPTY) {
      return null;
    }
    if (pattern.getStorage() == MaskingStorage.MEMORY) {
      return stores.computeIfAbsent("memory", key -> new MemoryMaskingStore());
    }
    String connectionName = resolve(pattern.getConnection());
    String schema = resolve(pattern.getSchemaName());
    String table = resolve(pattern.getTableName());
    if (StringUtils.isEmpty(connectionName) || StringUtils.isEmpty(table)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Check.NoConnection", pattern.getName()));
    }
    String storeKey = connectionName + "\t" + schema + "\t" + table;
    IMaskingStore existing = stores.get(storeKey);
    if (existing != null) {
      return existing;
    }
    DatabaseMeta databaseMeta = loadConnection(connectionName, pattern.getName());
    DatabaseMaskingStore store = new DatabaseMaskingStore(this, this, databaseMeta, schema, table);
    try {
      store.open();
    } catch (HopException e) {
      store.close();
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Error.OpenStore", pattern.getName()), e);
    }
    stores.put(storeKey, store);
    return store;
  }

  private DatabaseMeta loadConnection(String name, String patternName) throws HopException {
    IHopMetadataProvider provider = getMetadataProvider();
    DatabaseMeta databaseMeta =
        provider == null ? null : provider.getSerializer(DatabaseMeta.class).load(name);
    if (databaseMeta == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Check.MissingConnection", name, patternName));
    }
    return databaseMeta;
  }

  private static void closeStores(Map<String, IMaskingStore> stores) {
    for (IMaskingStore store : stores.values()) {
      store.close();
    }
  }
}
