/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.avro.transforms.avroinput;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.InputStream;

public class AvroFileInput extends BaseTransform<AvroFileInputMeta, AvroFileInputData>
    implements ITransform<AvroFileInputMeta, AvroFileInputData> {
  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public AvroFileInput(
      TransformMeta transformMeta,
      AvroFileInputMeta meta,
      AvroFileInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if (row == null) {
      // No more input files...
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.dataFilenameField = resolve(meta.getDataFilenameField());
      if (StringUtils.isEmpty(data.dataFilenameField)) {
        throw new HopException(
            "Please specify a field to use as the source of data filenames to read");
      }
      if (getInputRowMeta().indexOfValue(data.dataFilenameField) < 0) {
        throw new HopException(
            "Data filename field '"
                + data.dataFilenameField
                + "' doesn't exist in the input of this transform");
      }

      data.rowsLimit = Const.toInt(resolve(meta.getRowsLimit()), -1);

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // Next file to read...
    //
    String filename = getInputRowMeta().getString(row, data.dataFilenameField, null);

    // Read Avro rows of data from the file...
    //
    try {
      try (InputStream inputStream = HopVfs.getInputStream(filename)) {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(inputStream, datumReader);
        while (fileStream.hasNext()) {
          GenericRecord genericRecord = fileStream.next();
          incrementLinesInput();

          // Output the Avro value as a generic record
          //
          Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
          int outputIndex = getInputRowMeta().size();
          outputRow[outputIndex] = genericRecord;
          putRow(data.outputRowMeta, outputRow);

          // Stop the loop in case we have a row limit set
          //
          if (data.rowsLimit > 0 && getLinesInput() >= data.rowsLimit) {
            break;
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error reading from file '" + filename + "'", e);
    }

    // Try to read another file on the next iteration
    //
    return true;
  }
}
