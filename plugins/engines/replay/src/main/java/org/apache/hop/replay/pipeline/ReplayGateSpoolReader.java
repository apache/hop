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

package org.apache.hop.replay.pipeline;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.manifest.SnapshotManifest;
import org.xerial.snappy.SnappyInputStream;

public class ReplayGateSpoolReader implements AutoCloseable {

  private final String spoolPath;
  private final IVariables variables;

  private SnapshotManifest manifest;
  private IRowMeta rowMeta;
  private DataInputStream dataInputStream;
  private InputStream rawInputStream;
  private InputStream decompressedStream;

  public ReplayGateSpoolReader(String spoolPath, IVariables variables) {
    this.spoolPath = spoolPath;
    this.variables = variables;
  }

  public SnapshotManifest open() throws Exception {
    FileObject manifestFile = HopVfs.getFileObject(spoolPath + "/SnapshotManifest.json", variables);
    if (!manifestFile.exists()) {
      throw new HopFileException(
          "Snapshot manifest does not exist at " + manifestFile.getPublicURIString());
    }

    try (InputStream in = HopVfs.getInputStream(manifestFile)) {
      String json = IOUtils.toString(in, StandardCharsets.UTF_8);
      this.manifest = SnapshotManifest.fromJson(json);
    }

    String dataFileName = manifest.getDataFile();
    FileObject dataFile = HopVfs.getFileObject(spoolPath + "/" + dataFileName, variables);
    if (!dataFile.exists()) {
      throw new HopFileException(
          "Spool data file does not exist at " + dataFile.getPublicURIString());
    }

    this.rawInputStream = HopVfs.getInputStream(dataFile);
    String compression = manifest.getCompression();
    if (ReplayDefaults.COMPRESSION_SNAPPY.equalsIgnoreCase(compression)) {
      this.decompressedStream = new SnappyInputStream(rawInputStream);
    } else if (ReplayDefaults.COMPRESSION_GZIP.equalsIgnoreCase(compression)) {
      this.decompressedStream = new GZIPInputStream(rawInputStream);
    } else {
      this.decompressedStream = rawInputStream;
    }

    this.dataInputStream =
        new DataInputStream(new BufferedInputStream(this.decompressedStream, 65536));
    this.rowMeta = new RowMeta(this.dataInputStream);
    return manifest;
  }

  public Object[] readRow() throws Exception {
    if (dataInputStream == null || rowMeta == null) {
      return null;
    }
    try {
      return rowMeta.readData(dataInputStream);
    } catch (HopEofException | SocketTimeoutException e) {
      return null;
    }
  }

  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  public SnapshotManifest getManifest() {
    return manifest;
  }

  @Override
  public void close() throws Exception {
    if (dataInputStream != null) {
      dataInputStream.close();
    }
    if (decompressedStream != null) {
      decompressedStream.close();
    }
    if (rawInputStream != null) {
      rawInputStream.close();
    }
  }
}
