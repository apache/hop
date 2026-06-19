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
 *
 */

package org.apache.hop.arrow.datastream.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.arrow.datastream.shared.ArrowBaseDataStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.DataStreamPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@GuiPlugin
@DataStreamPlugin(
    id = "ArrowRandomAccessFile",
    name = "Apache Arrow random access file",
    description = "Stream rows of data to an Apache Arrow random access file")
@Getter
@Setter
public class ArrowFileDataStream extends ArrowBaseDataStream {
  @GuiWidgetElement(
      order = "20000-arrow-file-data-stream-filename",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::ArrowFileDataStream.Filename.Label",
      toolTip = "i18n::ArrowFileDataStream.Filename.Tooltip")
  @HopMetadataProperty
  protected String filename;

  @GuiWidgetElement(
      order = "20100-arrow-file-data-stream-batch-size",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::ArrowFileDataStream.BufferSize.Label",
      toolTip = "i18n::ArrowFileDataStream.BufferSize.Tooltip")
  @HopMetadataProperty
  protected String batchSize;

  protected String realFilename;
  protected int realBatchSize;

  private ArrowFileWriter arrowFileWriter;
  private ArrowFileReader arrowFileReader;
  private VectorSchemaRoot readRootSchema;
  private Schema readSchema;
  private int readRowIndex;
  private List<FieldVector> readFieldVectors;
  private int batchReads;

  public ArrowFileDataStream() {
    DataStreamPlugin annotation = getClass().getAnnotation(DataStreamPlugin.class);
    this.pluginId = annotation.id();
    this.pluginName = annotation.name();
    rowBuffer = new ArrayList<>();
    batchSize = "500";
    filename = "${java.io.tmpdir}/file.arrow";
  }

  @SuppressWarnings("CopyConstructorMissesField")
  public ArrowFileDataStream(ArrowFileDataStream s) {
    this();
    this.filename = s.filename;
    this.batchSize = s.batchSize;
  }

  @Override
  public ArrowFileDataStream clone() {
    return new ArrowFileDataStream(this);
  }

  @Override
  public void initialize(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean writing,
      DataStreamMeta dataStreamMeta)
      throws HopException {
    super.initialize(variables, metadataProvider, writing, dataStreamMeta);
    realFilename = variables.resolve(filename);
    realBatchSize = Const.toInt(variables.resolve(batchSize), 500);
  }

  @Override
  public void setOutputDone() throws HopException {
    if (!rowBuffer.isEmpty()) {
      emptyBuffer();
    }
    try {
      arrowFileWriter.end();
    } catch (Exception e) {
      throw new HopException("Error ending Arrow random access file data stream", e);
    }
  }

  @Override
  public void close() {
    if (arrowFileReader != null) {
      try {
        arrowFileReader.close();
      } catch (IOException e) {
        // Ignore
      }
    }
    if (fileInputStream != null) {
      try {
        fileInputStream.close();
      } catch (IOException e) {
        // Ignore
      }
    }
    if (vectorSchemaRoot != null) {
      vectorSchemaRoot.close();
    }
    if (arrowFileWriter != null) {
      arrowFileWriter.close();
    }
    if (fileOutputStream != null) {
      try {
        fileOutputStream.close();
      } catch (IOException e) {
        // Ignore
      }
    }

    if (rootAllocator != null) {
      rootAllocator.close();
    }
  }

  @Override
  public void setRowMeta(IRowMeta rowMeta) throws HopException {
    if (!writing) {
      return;
    }
    this.rowMeta = rowMeta;

    initializeFileWriting();
  }

  private void initializeFileWriting() throws HopException {
    Schema writeSchema = buildSchema(rowMeta);
    vectorSchemaRoot = VectorSchemaRoot.create(writeSchema, rootAllocator);

    // Allocate room in the field vectors
    //
    allocateFieldVectorsSpace(vectorSchemaRoot, rowMeta, realBatchSize);

    try {
      fileOutputStream = new FileOutputStream(variables.resolve(filename));
    } catch (Exception e) {
      throw new HopException("Error writing to Arrow random access file data stream", e);
    }
    arrowFileWriter = new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());
    try {
      arrowFileWriter.start();
    } catch (Exception e) {
      throw new HopException("Error starting to write to Arrow random access file data stream", e);
    }
  }

  /**
   * Gets rowMeta
   *
   * @return value of rowMeta
   */
  @Override
  public IRowMeta getRowMeta() throws HopException {
    if (writing) {
      return rowMeta;
    }
    try {
      initializeStreamReading();
    } catch (Exception e) {
      throw new HopException(
          "Error reading row metadata from Arrow random access file data stream " + realFilename,
          e);
    }
    return this.rowMeta;
  }

  private void initializeStreamReading() throws HopException, IOException {
    realFilename = variables.resolve(filename);
    realBatchSize = Const.toInt(variables.resolve(batchSize), 500);

    File file = new File(realFilename);
    if (!file.exists()) {
      throw new HopException(
          "The Arrow random access file to read from doesn't exist: " + realFilename);
    }

    fileInputStream = new FileInputStream(realFilename);
    arrowFileReader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator);
    readRootSchema = arrowFileReader.getVectorSchemaRoot();
    readSchema = readRootSchema.getSchema();

    this.rowMeta = buildRowMeta(readSchema);
  }

  @Override
  public void writeRow(Object[] rowData) throws HopException {
    rowBuffer.add(rowData);
    if (rowBuffer.size() >= realBatchSize) {
      emptyBuffer();
    }
  }

  private void emptyBuffer() throws HopException {
    try {
      vectorSchemaRoot.setRowCount(rowBuffer.size());

      // Set the data in the field vectors for the rows in the buffer
      //
      for (int rowIndex = 0; rowIndex < rowBuffer.size(); rowIndex++) {
        convertHopRowToFieldVectorIndex(
            vectorSchemaRoot, rowMeta, rowIndex, rowBuffer.get(rowIndex));
      }
      // With values set on all field vectors, we can now write the batch.
      //
      arrowFileWriter.writeBatch();
    } catch (Exception e) {
      throw new HopException(
          "Error writing row to Arrow random access file data stream" + filename, e);
    } finally {
      // We're done. Fill the buffer up again.
      rowBuffer.clear();
    }
  }

  @Override
  public Object[] readRow() throws HopException {
    if (writing) {
      throw new HopException("When writing data you can't read rows from the same data stream.");
    }

    try {
      // See if there are more rows to populate.
      //
      int schemaBatchSize = readRootSchema.getRowCount();

      // If needed, read another batch
      //
      if (readRowIndex < schemaBatchSize || readNextBatch()) {
        return convertFieldVectorsToHopRow(readFieldVectors, rowMeta, readRowIndex++);
      } else {
        // No more data to be expected
        return null;
      }
    } catch (Exception e) {
      throw new HopException(
          "Error while reading a batch of rows from an Arrow random access file data stream", e);
    }
  }

  protected boolean readNextBatch() throws IOException {
    boolean readNext = arrowFileReader.loadNextBatch();
    batchReads++;
    readRootSchema = arrowFileReader.getVectorSchemaRoot();

    // This loop is for the rare cases where the batches contain 0 rows each.
    //
    while (readRootSchema.getRowCount() == 0 && readNext) {
      readNext = arrowFileReader.loadNextBatch();
      batchReads++;
      readRootSchema = arrowFileReader.getVectorSchemaRoot();
    }
    readFieldVectors = readRootSchema.getFieldVectors();
    readRowIndex = 0;

    return readNext;
  }
}
