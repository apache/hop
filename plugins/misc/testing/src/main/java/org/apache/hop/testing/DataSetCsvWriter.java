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

package org.apache.hop.testing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Streams rows to a data set CSV file. The header is written when the writer is opened so an empty
 * input still produces a valid (header-only) data set file.
 */
public class DataSetCsvWriter implements AutoCloseable {

  private final IRowMeta setRowMeta;
  private final String dataSetFilename;
  private OutputStream outputStream;
  private BufferedWriter writer;
  private CSVPrinter csvPrinter;
  private boolean closed;

  public DataSetCsvWriter(IVariables variables, DataSet dataSet, IRowMeta rowMeta)
      throws HopException {
    this.dataSetFilename = dataSet.getActualDataSetFilename(variables);
    this.setRowMeta = rowMeta.clone();
    DataSetCsvUtil.setValueFormats(this.setRowMeta);

    try {
      FileObject file = HopVfs.getFileObject(dataSetFilename);
      FileObject parent = file.getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
      outputStream = HopVfs.getOutputStream(file, false);
      writer = new BufferedWriter(new OutputStreamWriter(outputStream));
      csvPrinter = new CSVPrinter(writer, DataSetCsvUtil.getCsvFormat(this.setRowMeta));
    } catch (Exception e) {
      closeQuietly();
      throw new HopException("Unable to open data set file '" + dataSetFilename + "'", e);
    }
  }

  public void writeRow(Object[] row) throws HopException {
    if (closed) {
      throw new HopException("Data set CSV writer is already closed: " + dataSetFilename);
    }
    try {
      List<String> strings = new ArrayList<>(setRowMeta.size());
      for (int i = 0; i < setRowMeta.size(); i++) {
        IValueMeta valueMeta = setRowMeta.getValueMeta(i);
        strings.add(valueMeta.getString(row[i]));
      }
      csvPrinter.printRecord(strings);
    } catch (Exception e) {
      throw new HopException("Unable to write a row to data set file '" + dataSetFilename + "'", e);
    }
  }

  @Override
  public void close() throws HopException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      if (csvPrinter != null) {
        csvPrinter.flush();
        csvPrinter.close();
        csvPrinter = null;
      }
      if (writer != null) {
        writer.close();
        writer = null;
      }
      if (outputStream != null) {
        outputStream.close();
        outputStream = null;
      }
    } catch (IOException e) {
      throw new HopException("Error closing data set file '" + dataSetFilename + "'", e);
    }
  }

  private void closeQuietly() {
    try {
      close();
    } catch (HopException e) {
      // Best effort while handling an open failure
    }
  }
}
