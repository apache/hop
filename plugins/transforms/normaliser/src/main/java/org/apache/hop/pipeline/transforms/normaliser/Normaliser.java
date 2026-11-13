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

package org.apache.hop.pipeline.transforms.normaliser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.normaliser.NormaliserData.Placement;

/** Normalise de-normalised input data. */
public class Normaliser extends BaseTransform<NormaliserMeta, NormaliserData> {
  private static final Class<?> PKG = NormaliserMeta.class;

  public Normaliser(
      TransformMeta transformMeta,
      NormaliserMeta meta,
      NormaliserData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row from rowset, wait for our turn, indicate busy!
    if (r == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      if (!initialise()) {
        return false;
      }
    }

    String[] types = data.types;
    Placement[][] placements = data.placements;
    int[] copyFieldNrs = data.copyFieldNrs;
    int rowMetaSz = data.outputRowMeta.size();

    // One output row per type: the fields that are kept, the type, then the normalised fields.
    //
    for (int t = 0; t < types.length; t++) {
      Object[] outputRowData = new Object[rowMetaSz];
      int outputIndex = 0;
      for (int nr : copyFieldNrs) {
        outputRowData[outputIndex++] = r[nr];
      }
      outputRowData[outputIndex] = types[t];

      for (Placement placement : placements[t]) {
        Object value = r[placement.inputIndex()];
        IValueMeta target = placement.target();
        if (target != null) {
          // Either the same type in another storage, or text for a field of mixed types.
          value =
              target.getType() == placement.source().getType()
                  ? placement.source().convertToNormalStorageType(value)
                  : target.convertData(placement.source(), value);
        }
        outputRowData[placement.outputIndex()] = value;
      }

      putRow(data.outputRowMeta, outputRowData);
    }

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "Normaliser.Log.LineNumber") + getLinesRead());
    }

    return true;
  }

  /**
   * Works out, once, which fields are kept and where every normalised value goes.
   *
   * @return false when a field to normalise is missing from the input; the transform has stopped
   */
  private boolean initialise() throws HopException {
    data.inputRowMeta = getInputRowMeta();
    data.outputRowMeta = data.inputRowMeta.clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    List<NormaliserField> fields = meta.getFieldsToNormalise(data.inputRowMeta, this);

    // The key fields are kept when every other field is normalised, so they have to be there.
    //
    if (meta.isNormalisingAllFields()) {
      for (String key : meta.getKeyFieldNames(this)) {
        if (data.inputRowMeta.indexOfValue(key) < 0) {
          logError(BaseMessages.getString(PKG, "Normaliser.Log.CouldNotFindKeyFieldInRow", key));
          setErrors(1);
          stopAll();
          return false;
        }
      }
    }

    String duplicate = NormaliserMeta.getDuplicateMapping(fields);
    if (duplicate != null) {
      throw new HopException(duplicate);
    }

    // The normalised fields are the last ones of the output row, in this order.
    //
    List<String> normNames = NormaliserMeta.getNormalisedFieldNames(fields);
    int firstNormIndex = data.outputRowMeta.size() - normNames.size();

    // For every type, in the order it first appears, the fields that get normalised under it and
    // where each one goes: into the normalised field it names, whatever order they are listed in.
    //
    Map<String, List<Placement>> typeToPlacements = new LinkedHashMap<>();
    for (NormaliserField field : fields) {
      int dataFieldNr = data.inputRowMeta.indexOfValue(field.getName());
      if (dataFieldNr < 0) {
        logError(
            BaseMessages.getString(PKG, "Normaliser.Log.CouldNotFindFieldInRow", field.getName()));
        setErrors(1);
        stopAll();
        return false;
      }
      int outputIndex = firstNormIndex + normNames.indexOf(field.getNorm());
      IValueMeta source = data.inputRowMeta.getValueMeta(dataFieldNr);
      IValueMeta target = data.outputRowMeta.getValueMeta(outputIndex);
      boolean copy =
          source.getType() == target.getType()
              && source.getStorageType() == target.getStorageType();
      typeToPlacements
          .computeIfAbsent(field.getValue(), k -> new ArrayList<>())
          .add(new Placement(dataFieldNr, outputIndex, source, copy ? null : target));
    }
    data.types = typeToPlacements.keySet().toArray(new String[0]);
    data.placements =
        typeToPlacements.values().stream()
            .map(list -> list.toArray(new Placement[0]))
            .toArray(Placement[][]::new);

    // The fields that are not normalised are copied as they are.
    //
    Set<String> normalised = NormaliserMeta.getFieldNames(fields);
    data.copyFieldNrs =
        IntStream.range(0, data.inputRowMeta.size())
            .filter(
                i ->
                    !normalised.contains(data.inputRowMeta.getValueMeta(i).getName().toLowerCase()))
            .toArray();
    return true;
  }
}
