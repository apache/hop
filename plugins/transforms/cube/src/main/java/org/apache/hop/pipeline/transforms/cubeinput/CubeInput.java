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

package org.apache.hop.pipeline.transforms.cubeinput;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.zip.GZIPInputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cube.CubeFilename;

public class CubeInput extends BaseTransform<CubeInputMeta, CubeInputData> {

  private static final Class<?> PKG = CubeInputMeta.class;

  private int realRowLimit;

  public CubeInput(
      TransformMeta transformMeta,
      CubeInputMeta meta,
      CubeInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    if (first) {
      first = false;
      realRowLimit = Const.toIntExpanded(resolve(meta.getRowLimit()), 0);
    }

    while (true) {
      if (data.dis == null) {
        if (!meta.isFilenameInField() || !openNextFileFromRow()) {
          setOutputDone();
          return false;
        }
      }

      try {
        Object[] r = data.meta.readData(data.dis);
        putRow(data.meta, r);
        incrementLinesInput();

        if (realRowLimit > 0 && getLinesInput() >= realRowLimit) {
          setOutputDone();
          return false;
        }
      } catch (HopEofException eof) {
        closeFile();
        if (!meta.isFilenameInField()) {
          setOutputDone();
          return false;
        }
        continue;
      } catch (SocketTimeoutException e) {
        throw new HopException(e); // shouldn't happen on files
      }

      if (checkFeedback(getLinesInput()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "CubeInput.Log.LineNumber") + getLinesInput());
      }

      return true;
    }
  }

  /**
   * @return false when the upstream row stream is finished
   */
  private boolean openNextFileFromRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      return false;
    }
    String fieldName = resolve(meta.getFilenameField());
    int index = Utils.isEmpty(fieldName) ? -1 : getInputRowMeta().indexOfValue(fieldName);
    if (index < 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "CubeInputMeta.Exception.FilenameFieldNotFound", Const.NVL(fieldName, "")));
    }
    String filename = getInputRowMeta().getString(row, index);
    // Names from a field are complete paths. The transform-nr suffix is not applied to them.
    filename = CubeFilename.resolve(this, filename, false, getCopy());
    if (Utils.isEmpty(filename)) {
      throw new HopException(BaseMessages.getString(PKG, "CubeInputMeta.Exception.EmptyFilename"));
    }
    openFile(filename);
    return true;
  }

  private void openFile(String filename) throws HopException {
    closeFile();
    InputStream input = null;
    DataInputStream dataInput = null;
    try {
      input = HopVfs.getInputStream(filename, variables);
      input = new CountingInputStream(input);
      GZIPInputStream gzip = new GZIPInputStream(input);
      dataInput = new DataInputStream(gzip);
      IRowMeta layout = new RowMeta(dataInput);
      if (data.meta != null && !sameLayout(data.meta, layout)) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "CubeInputMeta.Exception.LayoutMismatch",
                filename,
                Const.NVL(data.referenceFilename, "")));
      }
      data.fis = input;
      data.zip = gzip;
      data.dis = dataInput;
      input = null;
      dataInput = null;
      if (data.meta == null) {
        data.meta = layout;
        data.referenceFilename = filename;
      }
      if (meta.isAddFilenameResult()) {
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                HopVfs.getFileObject(filename, variables),
                getPipelineMeta().getName(),
                toString());
        resultFile.setComment("File was read by a Cube Input transform");
        addResultFile(resultFile);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "CubeInput.Log.ErrorReadingFromDataCube") + filename, e);
    } finally {
      if (dataInput != null) {
        try {
          dataInput.close();
        } catch (IOException ignored) {
          // The transform is already failing. dispose() closes a file that was stored.
        }
      } else if (input != null) {
        try {
          input.close();
        } catch (IOException ignored) {
          // See above.
        }
      }
    }
  }

  private static boolean sameLayout(IRowMeta left, IRowMeta right) {
    if (left.size() != right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      IValueMeta a = left.getValueMeta(i);
      IValueMeta b = right.getValueMeta(i);
      if (!a.getName().equals(b.getName()) || a.getType() != b.getType()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (meta.isFilenameInField()) {
        return true;
      }
      try {
        String filename =
            CubeFilename.resolve(
                this, meta.getFilename(), meta.usesTransformNrInFilename(), getCopy());
        openFile(filename);
        return true;
      } catch (HopFileException kfe) {
        logError(BaseMessages.getString(PKG, "CubeInput.Log.UnableToReadMetadata"), kfe);
        return false;
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "CubeInput.Log.ErrorReadingFromDataCube"), e);
      }
    }
    return false;
  }

  private void closeFile() {
    if (data.fis instanceof CountingInputStream cis) {
      dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + cis.getCount();
    }
    try {
      if (data.dis != null) {
        data.dis.close();
      }
    } catch (IOException e) {
      logError(BaseMessages.getString(PKG, "CubeInput.Log.ErrorClosingCube") + e.toString());
      setErrors(1);
      stopAll();
    } finally {
      data.dis = null;
      data.zip = null;
      data.fis = null;
    }
  }

  @Override
  public void dispose() {
    closeFile();
    super.dispose();
  }
}
