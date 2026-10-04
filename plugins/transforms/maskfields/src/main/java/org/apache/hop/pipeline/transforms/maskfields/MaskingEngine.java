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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.maskfields.store.IMaskingStore;

/** Applies resolved masking patterns to one row. The field type is left unchanged. */
public class MaskingEngine {

  private static final Class<?> PKG = MaskingEngine.class;

  private final List<Binding> bindings;
  private final List<IMaskingStore> stores;
  private final ValueMetaString maskedString = new ValueMetaString("masked");

  public MaskingEngine(List<Binding> bindings, List<IMaskingStore> stores) {
    this.bindings = bindings;
    this.stores = stores;
  }

  public List<Binding> getBindings() {
    return bindings;
  }

  public void apply(IRowMeta rowMeta, Object[] row) throws HopException {
    for (Binding binding : bindings) {
      int index = rowMeta.indexOfValue(binding.fieldName);
      if (index < 0) {
        throw new HopException(
            BaseMessages.getString(PKG, "MaskFields.Error.FieldMissing", binding.fieldName));
      }
      IValueMeta valueMeta = rowMeta.getValueMeta(index);
      try {
        row[index] = mask(binding, valueMeta, row[index]);
      } catch (HopValueException e) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MaskFields.Error.Convert", binding.fieldName, binding.pattern.getName()),
            e);
      }
    }
  }

  public void close() {
    for (IMaskingStore store : stores) {
      store.close();
    }
  }

  private Object mask(Binding binding, IValueMeta valueMeta, Object value)
      throws HopException, HopValueException {
    MaskingPattern pattern = binding.pattern;
    MaskingValueSource source = pattern.getValueSource();
    if (source == MaskingValueSource.SET_NULL) {
      return null;
    }
    if (source == MaskingValueSource.SET_EMPTY) {
      return "";
    }
    String key = valueMeta.getString(value);
    if (StringUtils.isEmpty(key)) {
      return value;
    }
    if (source == MaskingValueSource.LIST) {
      return toField(valueMeta, listValue(binding, key));
    }
    return toField(valueMeta, syntheticValue(binding, valueMeta, key));
  }

  private String syntheticValue(Binding binding, IValueMeta valueMeta, String key)
      throws HopException {
    if (!binding.pattern.remembers()) {
      return formatSynthetic(binding, valueMeta, nextToken(binding));
    }
    return binding.store.findOrCreate(
        binding.pattern.getName(),
        key,
        store -> formatSynthetic(binding, valueMeta, nextToken(binding, store)));
  }

  private String nextToken(Binding binding) {
    if (binding.pattern.getToken() == MaskingToken.UUID) {
      return UUID.randomUUID().toString();
    }
    return Long.toString(binding.localSequence.getAndIncrement());
  }

  private String nextToken(Binding binding, IMaskingStore store) throws HopException {
    if (binding.pattern.getToken() == MaskingToken.UUID) {
      return UUID.randomUUID().toString();
    }
    return Long.toString(store.allocateSequence(binding.pattern.getName(), binding.sequenceStart));
  }

  private String formatSynthetic(Binding binding, IValueMeta valueMeta, String token) {
    if (valueMeta.isString()) {
      return binding.prefix + token + binding.suffix;
    }
    return token;
  }

  private String listValue(Binding binding, String key) throws HopException {
    List<String> values = binding.listValues;
    if (values.isEmpty()) {
      throw new HopException(
          BaseMessages.getString(PKG, "MaskFields.Error.EmptyList", binding.pattern.getName()));
    }
    if (!binding.pattern.remembers()) {
      int index = Math.floorMod(binding.roundRobin.getAndIncrement(), values.size());
      return values.get(index);
    }
    return binding.store.findOrCreate(
        binding.pattern.getName(),
        key,
        store -> {
          int index =
              Math.floorMod(store.allocateSequence(binding.pattern.getName(), 0), values.size());
          return values.get(index);
        });
  }

  private Object toField(IValueMeta target, String masked) throws HopValueException {
    if (target.isString()) {
      return masked;
    }
    return target.convertData(maskedString, masked);
  }

  /** One field, already resolved against its pattern. */
  public static final class Binding {
    final String fieldName;
    final MaskingPattern pattern;
    final String prefix;
    final String suffix;
    final long sequenceStart;
    final IMaskingStore store;
    final List<String> listValues = new ArrayList<>();
    final AtomicInteger roundRobin = new AtomicInteger();
    final AtomicLong localSequence;

    public Binding(
        String fieldName,
        MaskingPattern pattern,
        String prefix,
        String suffix,
        long sequenceStart,
        IMaskingStore store) {
      this.fieldName = fieldName;
      this.pattern = pattern;
      this.prefix = prefix == null ? "" : prefix;
      this.suffix = suffix == null ? "" : suffix;
      this.sequenceStart = sequenceStart;
      this.store = store;
      this.localSequence = new AtomicLong(sequenceStart);
    }
  }
}
