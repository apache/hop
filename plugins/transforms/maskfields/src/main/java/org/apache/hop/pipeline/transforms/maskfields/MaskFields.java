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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.maskfields.MaskingEngine.Binding;
import org.apache.hop.pipeline.transforms.maskfields.store.IMaskingStore;

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
    MaskingRuntime.Lease lease = MaskingRuntime.getInstance().acquire(executionId());
    data.lease = lease;
    try {
      List<Binding> bindings = new ArrayList<>();
      data.patterns = new LinkedHashMap<>();
      if (meta.getFields() != null) {
        for (MaskField field : meta.getFields()) {
          if (field == null
              || StringUtils.isEmpty(field.getFieldName())
              || StringUtils.isEmpty(field.getPatternName())) {
            continue;
          }
          bindings.add(bind(field));
        }
      }
      data.catalogFields =
          meta.activeTagRules().isEmpty()
              ? Map.of()
              : new CatalogFieldResolver(this, getLogChannel(), meta, getMetadataProvider())
                  .resolve();
      for (CatalogFieldResolver.Match match : data.catalogFields.values()) {
        // A missing pattern fails now, not at the first row
        pattern(match.patternName());
      }
      data.engine = new MaskingEngine(bindings);
      IPipelineEngine<PipelineMeta> pipeline = getPipeline();
      if (pipeline != null) {
        pipeline.addExecutionFinishedListener(engine -> lease.release());
      }
      return true;
    } catch (HopException e) {
      lease.release();
      data.lease = null;
      logError(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    if (data.outputRowMeta == null) {
      data.outputRowMeta = getInputRowMeta().clone();
      bindCatalogFields(data.outputRowMeta);
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
    if (data.lease != null) {
      data.lease.release();
      data.lease = null;
    }
    super.dispose();
  }

  /** One field and its pattern, ready to mask. */
  private Binding bind(MaskField field) throws HopException {
    MaskingPattern pattern = pattern(field.getPatternName());
    long start = parseStart(pattern);
    // Check the hash secret before the mapping table is opened.
    MaskingKey key = MaskingKey.forPattern(pattern, this);
    IMaskingStore store = storeFor(pattern, data.lease);
    String transformName = getTransformMeta() == null ? "" : getTransformMeta().getName();
    return new Binding(
        field.getFieldName(),
        pattern,
        resolve(pattern.getPrefix()),
        resolve(pattern.getSuffix()),
        start,
        store,
        sharedSequence(data.lease, transformName, field, pattern, start),
        key);
  }

  private MaskingPattern pattern(String name) throws HopException {
    MaskingPattern pattern = data.patterns.get(name);
    if (pattern == null) {
      pattern = loadRequired(name);
      data.patterns.put(name, pattern);
    }
    return pattern;
  }

  /**
   * Masks the incoming fields the data catalog labels with a tag or term of this transform. Done at
   * the first row, from the fields that really arrive: a field that only shows up at run time is
   * masked too. A field with a row of its own keeps the pattern of that row.
   */
  private void bindCatalogFields(IRowMeta rowMeta) throws HopException {
    if (data.catalogFields.isEmpty()) {
      return;
    }
    Set<String> explicit = new HashSet<>();
    for (Binding binding : data.engine.getBindings()) {
      explicit.add(binding.fieldName.toLowerCase(Locale.ROOT));
    }
    Set<String> arrived = new HashSet<>();
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      String key = valueMeta.getName().toLowerCase(Locale.ROOT);
      arrived.add(key);
      CatalogFieldResolver.Match match = data.catalogFields.get(key);
      if (match == null || explicit.contains(key)) {
        continue;
      }
      data.engine.getBindings().add(bind(new MaskField(valueMeta.getName(), match.patternName())));
      logBasic(
          BaseMessages.getString(
              PKG,
              "MaskFields.Log.CatalogField",
              valueMeta.getName(),
              match.patternName(),
              match.label()));
    }
    for (CatalogFieldResolver.Match match : data.catalogFields.values()) {
      if (!arrived.contains(match.fieldName().toLowerCase(Locale.ROOT))) {
        logDetailed(
            BaseMessages.getString(PKG, "MaskFields.Log.CatalogFieldAbsent", match.fieldName()));
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

  private AtomicLong sharedSequence(
      MaskingRuntime.Lease lease,
      String transformName,
      MaskField field,
      MaskingPattern pattern,
      long start) {
    if (pattern.remembers()
        || pattern.getValueSource() != MaskingValueSource.SYNTHETIC
        || pattern.getToken() == MaskingToken.UUID) {
      return null;
    }
    return lease.sequence(transformName, field.getFieldName(), start);
  }

  private String executionId() {
    IPipelineEngine<PipelineMeta> pipeline = getPipeline();
    if (pipeline != null && StringUtils.isNotEmpty(pipeline.getLogChannelId())) {
      return pipeline.getLogChannelId();
    }
    return "mask-fields-" + System.identityHashCode(this);
  }

  private IMaskingStore storeFor(MaskingPattern pattern, MaskingRuntime.Lease lease)
      throws HopException {
    if (!pattern.remembers()
        || pattern.getValueSource() == MaskingValueSource.SET_NULL
        || pattern.getValueSource() == MaskingValueSource.SET_EMPTY) {
      return null;
    }
    if (pattern.getStorage() == MaskingStorage.MEMORY) {
      return lease.memory();
    }
    String connectionName = resolve(pattern.getConnection());
    String schema = resolve(pattern.getSchemaName());
    String table = resolve(pattern.getTableName());
    if (StringUtils.isEmpty(connectionName) || StringUtils.isEmpty(table)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Check.NoConnection", pattern.getName()));
    }
    DatabaseMeta databaseMeta = loadConnection(connectionName, pattern.getName());
    try {
      return lease.database(this, this, databaseMeta, schema, table);
    } catch (HopException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Error.OpenStore", pattern.getName()), e);
    }
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
}
