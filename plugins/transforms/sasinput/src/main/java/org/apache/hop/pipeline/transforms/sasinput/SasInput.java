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

package org.apache.hop.pipeline.transforms.sasinput;

import com.epam.parso.Column;
import com.epam.parso.ColumnFormat;
import com.epam.parso.SasFileProperties;
import com.epam.parso.impl.SasFileReaderImpl;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/** Reads data from a SAS file in SAS7BAT format. */
public class SasInput extends BaseTransform<SasInputMeta, SasInputData> {
  private static final Class<?> PKG = SasInputMeta.class; // for Translator

  public SasInput(
      TransformMeta stepMeta,
      SasInputMeta meta,
      SasInputData data,
      int copyNr,
      PipelineMeta transMeta,
      Pipeline pipeline) {
    super(stepMeta, meta, data, copyNr, transMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    final Object[] fileRowData = getRow();
    if (fileRowData == null) {
      // No more work to do...
      //
      setOutputDone();
      return false;
    }

    // First we see if we need to get a list of files from input...
    //
    if (first) {

      // The output row meta data, what does it look like?
      //
      data.outputRowMeta = new RowMeta();

      // See if the input row contains the filename field...
      //
      int idx = getInputRowMeta().indexOfValue(meta.getAcceptingField());
      if (idx < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "SASInput.Log.Error.UnableToFindFilenameField", meta.getAcceptingField()));
      }

      // Determine the output row layout
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    String rawFilename = getInputRowMeta().getString(fileRowData, meta.getAcceptingField(), null);
    final String filename = resolve(rawFilename);

    // Add this to the result file names...
    //
    ResultFile resultFile =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL,
            HopVfs.getFileObject(filename),
            getPipelineMeta().getName(),
            getTransformName());
    resultFile.setComment(BaseMessages.getString(PKG, "SASInput.ResultFile.Comment"));
    addResultFile(resultFile);

    // Read the SAS File
    //
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      SasFileReaderImpl sasFileReader = new SasFileReaderImpl(inputStream);
      SasFileProperties sasFileProperties = sasFileReader.getSasFileProperties();

      logBasic(BaseMessages.getString(PKG, "SASInput.Log.OpenedSASFile") + " : [" + filename + "]");

      // What are the columns in the file?
      //
      List<Column> columns = sasFileReader.getColumns();

      // Map this to the columns we want...
      //
      List<Integer> indexes = new ArrayList<>();
      for (SasInputField field : meta.getOutputFields()) {

        int index = -1;
        for (int c = 0; c < columns.size(); c++) {
          if (columns.get(c).getName().equalsIgnoreCase(field.getName())) {
            index = c;
            break;
          }
        }
        if (index < 0) {
          throw new HopException(
              "Field '" + field.getName() + " could not be found in input file '" + filename);
        }
        indexes.add(index);
      }

      // Now we have the indexes of the output fields to grab.
      // Let's grab them...
      //
      Object[] sasRow;
      while ((sasRow = sasFileReader.readNext()) != null) {
        Object[] outputRow = RowDataUtil.createResizedCopy(fileRowData, data.outputRowMeta.size());

        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          SasInputField field = meta.getOutputFields().get(i);
          int index = indexes.get(i);
          Column column = columns.get(index);
          ColumnFormat columnFormat = column.getFormat();
          Object sasValue = sasRow[index];
          Object value = null;
          IValueMeta inputValueMeta = null;
          String fieldName = Const.NVL(field.getRename(), field.getName());
          int outputIndex = getInputRowMeta().size() + i;
          if (sasValue instanceof byte[]) {
            inputValueMeta = new ValueMetaString(fieldName);
            if (sasFileProperties.getEncoding() != null) {
              value = new String((byte[]) sasValue, sasFileProperties.getEncoding());
            } else {
              // TODO: user defined encoding.
              value = new String((byte[]) sasValue);
            }
          }
          if (sasValue instanceof String) {
            inputValueMeta = new ValueMetaString(fieldName);
            value = sasValue;
          }
          if (sasValue instanceof Double) {
            inputValueMeta = new ValueMetaNumber(fieldName);
            value = sasValue;
          }
          if (sasValue instanceof Float) {
            inputValueMeta = new ValueMetaNumber(fieldName);
            value = Double.valueOf((double) sasValue);
          }
          if (sasValue instanceof Long) {
            inputValueMeta = new ValueMetaInteger(fieldName);
            value = sasValue;
          }
          if (sasValue instanceof Integer) {
            inputValueMeta = new ValueMetaInteger(fieldName);
            value = Long.valueOf((int) sasValue);
          }
          if (sasValue instanceof Date) {
            inputValueMeta = new ValueMetaDate(fieldName);
            value = sasValue;
          }
          if (inputValueMeta != null) {
            inputValueMeta.setLength(field.getLength());
            inputValueMeta.setPrecision(field.getPrecision());
            inputValueMeta.setConversionMask(field.getConversionMask());
            IValueMeta outputValueMeta = data.outputRowMeta.getValueMeta(outputIndex);
            outputRow[outputIndex] = outputValueMeta.convertData(inputValueMeta, value);
          }
        }

        // Send the row on its way...
        //
        putRow(data.outputRowMeta, outputRow);
      }
    } catch (Exception e) {
      throw new HopException("Error reading from file " + filename, e);
    }

    return true;
  }
}
