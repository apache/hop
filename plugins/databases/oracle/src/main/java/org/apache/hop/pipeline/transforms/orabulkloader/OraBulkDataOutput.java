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
package org.apache.hop.pipeline.transforms.orabulkloader;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Does the opening of the output "stream". It's either a file or inter process communication which
 * is transparent to users of this class.
 */
public class OraBulkDataOutput {
  private OraBulkLoaderMeta meta;
  private Writer output = null;
  private StringBuilder outbuf = null;
  private boolean first = true;
  private int[] fieldNumbers = null;
  private String enclosure = null;
  private SimpleDateFormat sdfDate = null;
  private SimpleDateFormat sdfDateTime = null;
  private String recTerm;

  public OraBulkDataOutput(OraBulkLoaderMeta meta, String recTerm) {
    this.meta = meta;
    this.recTerm = recTerm;
  }

  public void open(IVariables variables, Process sqlldrProcess) throws HopException {
    String loadMethod = meta.getLoadMethod();
    try {
      OutputStream os;

      if (OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals(loadMethod)) {
        os = sqlldrProcess.getOutputStream();
      } else {
        // Else open the data file filled in.
        String dataFilePath = getFilename(getFileObject(meta.getDataFile(), variables));
        File dataFile = new File(dataFilePath);
        // Make sure the parent directory exists
        dataFile.getParentFile().mkdirs();
        os = new FileOutputStream(dataFile, false);
      }

      String encoding = meta.getEncoding();
      if (Utils.isEmpty(encoding)) {
        // Use the default encoding.
        output = new BufferedWriter(new OutputStreamWriter(os));
      } else {
        // Use the specified encoding
        output = new BufferedWriter(new OutputStreamWriter(os, encoding));
      }
    } catch (IOException e) {
      throw new HopException("IO exception occured: " + e.getMessage(), e);
    }
  }

  public void close() throws IOException {
    if (output != null) {
      output.close();
    }
  }

  Writer getOutput() {
    return output;
  }

  private String createEscapedString(String orig, String enclosure) {
    StringBuilder buf = new StringBuilder(orig);

    Const.repl(buf, enclosure, enclosure + enclosure);
    return buf.toString();
  }

  public void writeLine(IRowMeta rowMeta, Object[] row) throws HopException {
    if (first) {
      first = false;

      enclosure = meta.getEnclosure();

      // Setup up the fields we need to take for each of the rows
      // as this speeds up processing.
      fieldNumbers = new int[meta.getMappings().size()];
      for (int i = 0; i < fieldNumbers.length; i++) {
        fieldNumbers[i] = rowMeta.indexOfValue(meta.getMappings().get(i).getFieldStream());
        if (fieldNumbers[i] < 0) {
          throw new HopException(
              "Could not find field " + meta.getMappings().get(i).getFieldStream() + " in stream");
        }
      }

      sdfDate = new SimpleDateFormat("yyyy-MM-dd");
      sdfDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

      outbuf = new StringBuilder();
    }
    outbuf.setLength(0);

    // Write the data to the output
    IValueMeta valueMeta;
    int number;
    for (int i = 0; i < fieldNumbers.length; i++) {
      if (i != 0) {
        outbuf.append(',');
      }
      number = fieldNumbers[i];
      valueMeta = rowMeta.getValueMeta(number);
      if (row[number] == null) {
        // TODO (SB): special check for null in case of Strings.
        outbuf.append(enclosure);
        outbuf.append(enclosure);
      } else {
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_STRING:
            String s = rowMeta.getString(row, number);
            outbuf.append(enclosure);
            if (null != s) {
              if (s.contains(enclosure)) {
                s = createEscapedString(s, enclosure);
              }
              outbuf.append(s);
            }
            outbuf.append(enclosure);
            break;
          case IValueMeta.TYPE_INTEGER:
            Long l = rowMeta.getInteger(row, number);
            outbuf.append(enclosure);
            if (null != l) {
              outbuf.append(l);
            }
            outbuf.append(enclosure);
            break;
          case IValueMeta.TYPE_NUMBER:
            Double d = rowMeta.getNumber(row, number);
            outbuf.append(enclosure);
            if (null != d) {
              outbuf.append(d);
            }
            outbuf.append(enclosure);
            break;
          case IValueMeta.TYPE_BIGNUMBER:
            BigDecimal bd = rowMeta.getBigNumber(row, number);
            outbuf.append(enclosure);
            if (null != bd) {
              outbuf.append(bd);
            }
            outbuf.append(enclosure);
            break;
          case IValueMeta.TYPE_DATE:
            Date dt = rowMeta.getDate(row, number);
            outbuf.append(enclosure);
            if (null != dt) {
              String mask = meta.getMappings().get(i).getDateMask();
              if (OraBulkLoaderMeta.DATE_MASK_DATETIME.equals(mask)) {
                outbuf.append(sdfDateTime.format(dt));
              } else {
                // Default is date format
                outbuf.append(sdfDate.format(dt));
              }
            }
            outbuf.append(enclosure);
            break;
          case IValueMeta.TYPE_BOOLEAN:
            Boolean b = rowMeta.getBoolean(row, number);
            outbuf.append(enclosure);
            if (null != b) {
              if (b) {
                outbuf.append('Y');
              } else {
                outbuf.append('N');
              }
            }
            outbuf.append(enclosure);
            break;
          case IValueMeta.TYPE_BINARY:
            byte[] byt = rowMeta.getBinary(row, number);
            outbuf.append("<startlob>");
            // TODO REVIEW - implicit .toString
            outbuf.append(byt);
            outbuf.append("<endlob>");
            break;
          case IValueMeta.TYPE_TIMESTAMP:
            Timestamp timestamp = (Timestamp) rowMeta.getDate(row, number);
            outbuf.append(enclosure);
            if (null != timestamp) {
              outbuf.append(timestamp.toString());
            }
            outbuf.append(enclosure);
            break;
          default:
            throw new HopException("Unsupported type");
        }
      }
    }
    outbuf.append(recTerm);
    try {
      output.append(outbuf);
    } catch (IOException e) {
      throw new HopException("IO exception occured: " + e.getMessage(), e);
    }
  }

  @VisibleForTesting
  String getFilename(FileObject fileObject) {
    return HopVfs.getFilename(fileObject);
  }

  @VisibleForTesting
  FileObject getFileObject(String fileName, IVariables variables) throws HopFileException {
    return HopVfs.getFileObject(variables.resolve(fileName), variables);
  }
}
