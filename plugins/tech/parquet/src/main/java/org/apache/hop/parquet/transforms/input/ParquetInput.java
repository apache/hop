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

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class ParquetInput extends BaseTransform<ParquetInputMeta, ParquetInputData>
{
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
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.filenameFieldIndex = getInputRowMeta().indexOfValue(resolve(meta.getFilenameField()));
      if (data.filenameFieldIndex < 0) {
        throw new HopException(
            "Unable to find filename field " + meta.getFilenameField() + " in the input");
      }
    }

    // Skip null values for file names
    //
    if (getInputRowMeta().isNull(row, data.filenameFieldIndex)) {
      return true;
    }

    String filename = getInputRowMeta().getString(row, data.filenameFieldIndex);
    FileObject fileObject = HopVfs.getFileObject(filename);

    try {
      long size = fileObject.getContent().getSize();
      InputStream inputStream = HopVfs.getInputStream(fileObject);

      // Reads the whole file into memory...
      //
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) size);
      IOUtils.copy(inputStream, outputStream);
      ParquetStream inputFile = new ParquetStream(outputStream.toByteArray(), filename);

      ParquetReadSupport readSupport = new ParquetReadSupport(meta.getFields());
      ParquetReader<RowMetaAndData> reader =
          new ParquetReaderBuilder<>(readSupport, inputFile).build();

      RowMetaAndData r = reader.read();
      while (r != null && !isStopped()) {
        // Add r to the input rows...
        //
        Object[] outputRow = RowDataUtil.addRowData(row, getInputRowMeta().size(), r.getData());
        putRow(data.outputRowMeta, outputRow);
        r = reader.read();
      }
    } catch (Exception e) {
      throw new HopException("Error read file " + filename, e);
    }

    return true;
  }
}
