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

package org.apache.hop.parquet.transforms.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class ParquetInput extends BaseTransform<ParquetInputMeta, ParquetInputData> {
  public ParquetInput(
      TransformMeta transformMeta,
      ParquetInputMeta meta,
      ParquetInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      // No more files, we're done.
      closeFile();

      // Do we need the file metadata and the file was empty?
      //
      if (meta.isSendingNullsRowWhenEmpty() && getLinesInput() == 0) {
        if (data.outputRowMeta == null) {
          // No file name ever came in, so the output is just the fields of this transform.
          data.outputRowMeta =
              getInputRowMeta() == null ? new RowMeta() : getInputRowMeta().clone();
          meta.getFields(
              data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
        }
        putRow(data.outputRowMeta, RowDataUtil.allocateRowData(data.outputRowMeta.size()));
      }

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.filenameFieldIndex = getInputRowMeta().indexOfValue(resolve(meta.getFilenameField()));
      if (data.filenameFieldIndex < 0) {
        throw new HopException(
            "Unable to find filename field " + meta.getFilenameField() + " in the input");
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // Skip null values for file names
    //
    if (getInputRowMeta().isNull(row, data.filenameFieldIndex)) {
      return true;
    }

    String filename = getInputRowMeta().getString(row, data.filenameFieldIndex);
    FileObject fileObject = HopVfs.getFileObject(filename, variables);

    try {
      List<ParquetField> fields = new ArrayList<>(meta.getFields());

      // If we don't have any fields specified, we read them all.
      //
      if (fields.isEmpty()) {
        IRowMeta parquetRowMeta = ParquetInputMeta.extractRowMeta(this, filename);
        if (data.outputRowMeta.size() == getInputRowMeta().size()) {
          // Nothing was known at design time (no fields and no metadata file), so the first
          // file's schema describes the values this transform appends to the row.
          data.outputRowMeta.addRowMeta(parquetRowMeta);
        }
        for (int i = 0; i < parquetRowMeta.size(); i++) {
          IValueMeta parquetValueMeta = parquetRowMeta.getValueMeta(i);
          fields.add(
              new ParquetField(
                  parquetValueMeta.getName(),
                  parquetValueMeta.getName(),
                  parquetValueMeta.getTypeDesc(),
                  parquetValueMeta.getFormatMask(),
                  Integer.toString(parquetValueMeta.getLength()),
                  Integer.toString(parquetValueMeta.getPrecision())));
        }
      }

      long size = fileObject.getContent().getSize();
      dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + size;
      if (size > 0) {
        try {
          LineageFileIoEmitter.emitTransformFileIo(
              this, FileIoOperation.READ, fileObject, null, size, true, null);
        } catch (Exception ignored) {
          // optional lineage
        }
      }

      data.parquetStream = new ParquetStream(fileObject, filename);

      ParquetReadSupport readSupport = new ParquetReadSupport(fields);
      data.reader = new ParquetReaderBuilder<>(readSupport, data.parquetStream).build();

      RowMetaAndData r = data.reader.read();
      while (r != null && !isStopped()) {
        incrementLinesInput();
        // Add r to the input rows...
        //
        Object[] outputRow = RowDataUtil.addRowData(row, getInputRowMeta().size(), r.getData());
        putRow(data.outputRowMeta, outputRow);
        r = data.reader.read();
      }
    } catch (Exception e) {
      throw new HopException("Error read file " + filename, e);
    } finally {
      // Every file gets its own reader; release this one before the next file name comes in.
      closeFile();
    }

    return true;
  }

  /** Closes the reader and stream of the file being read, if any. Safe to call more than once. */
  public void closeFile() {
    try {
      if (data.reader != null) {
        data.reader.close();
      }
      if (data.parquetStream != null) {
        data.parquetStream.close();
      }
    } catch (IOException e) {
      logError("Unable to properly close parquet reader!", e);
    } finally {
      data.reader = null;
      data.parquetStream = null;
    }
  }

  @Override
  public void dispose() {
    super.dispose();

    closeFile();
  }
}
