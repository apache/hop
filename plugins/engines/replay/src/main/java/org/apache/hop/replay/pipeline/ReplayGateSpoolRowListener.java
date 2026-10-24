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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.replay.manifest.SnapshotFieldMeta;
import org.apache.hop.replay.manifest.SnapshotManifest;
import org.xerial.snappy.SnappyOutputStream;

public class ReplayGateSpoolRowListener extends RowAdapter {

  @Getter private final IEngineComponent component;
  @Getter private final ReplayGate gate;
  @Getter private final String pipelineName;
  private final IVariables variables;
  private final int totalCopies;
  private final ILogChannel log;

  @Getter private final AtomicLong rowCount = new AtomicLong(0L);
  private boolean firstRow = true;
  @Getter @Setter private IRowMeta savedRowMeta;
  @Getter private boolean closed = false;
  @Getter private boolean errorOccurred = false;
  @Getter private String dataFileName;

  private OutputStream rawOutputStream;
  private OutputStream compressedOutputStream;
  private DigestOutputStream digestOutputStream;
  private DataOutputStream dataOutputStream;
  private MessageDigest digest;

  public ReplayGateSpoolRowListener(
      IEngineComponent component,
      ReplayGate gate,
      String pipelineName,
      IVariables variables,
      int totalCopies,
      ILogChannel log) {
    this.component = component;
    this.gate = gate;
    this.pipelineName = pipelineName;
    this.variables = variables;
    this.totalCopies = totalCopies;
    this.log = log;
  }

  @Override
  public synchronized void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
      throws HopTransformException {
    if (closed || errorOccurred) {
      return;
    }
    try {
      if (firstRow) {
        initOutputStream(rowMeta);
        firstRow = false;
      }
      if (gate.getRowLimit() > 0 && rowCount.get() >= gate.getRowLimit()) {
        return;
      }
      savedRowMeta.writeData(dataOutputStream, row);
      rowCount.incrementAndGet();
    } catch (Exception e) {
      errorOccurred = true;
      log.logError("Error spooling row in replay gate for transform " + component.getName(), e);
      throw new HopTransformException("Error spooling row in replay gate", e);
    }
  }

  @Override
  public void errorRowWrittenEvent(IRowMeta rowMeta, Object[] row) {
    errorOccurred = true;
  }

  public synchronized void initOutputStream(IRowMeta rowMeta) throws Exception {
    if (dataOutputStream != null) {
      return;
    }
    this.savedRowMeta = rowMeta.clone();
    String targetFolder =
        ReplayGateUtil.getTransformSpoolPath(variables, gate, pipelineName, component.getName());
    FileObject folderObj = HopVfs.getFileObject(targetFolder, variables);
    if (!folderObj.exists()) {
      folderObj.createFolder();
    }

    String ext = ".bin";
    String compression = gate.getCompression();
    if (StringUtils.isEmpty(compression)) {
      compression = ReplayDefaults.DEFAULT_COMPRESSION;
    }

    if (ReplayDefaults.COMPRESSION_SNAPPY.equalsIgnoreCase(compression)) {
      ext = ".bin.snappy";
    } else if (ReplayDefaults.COMPRESSION_GZIP.equalsIgnoreCase(compression)) {
      ext = ".bin.gz";
    }

    this.dataFileName =
        (component.getCopyNr() == 0 && totalCopies <= 1)
            ? ("data" + ext)
            : ("data_" + component.getCopyNr() + ext);

    FileObject dataFileObj = HopVfs.getFileObject(targetFolder + "/" + dataFileName, variables);
    this.rawOutputStream = HopVfs.getOutputStream(dataFileObj, false);

    this.digest = MessageDigest.getInstance("SHA-256");
    this.digestOutputStream = new DigestOutputStream(this.rawOutputStream, this.digest);

    if (ReplayDefaults.COMPRESSION_SNAPPY.equalsIgnoreCase(compression)) {
      this.compressedOutputStream = new SnappyOutputStream(this.digestOutputStream);
    } else if (ReplayDefaults.COMPRESSION_GZIP.equalsIgnoreCase(compression)) {
      this.compressedOutputStream = new GZIPOutputStream(this.digestOutputStream);
    } else {
      this.compressedOutputStream = this.digestOutputStream;
    }

    this.dataOutputStream =
        new DataOutputStream(new BufferedOutputStream(this.compressedOutputStream, 65536));
    this.savedRowMeta.writeMeta(this.dataOutputStream);
  }

  public synchronized void close(boolean hasErrors) {
    if (closed) {
      return;
    }
    closed = true;
    try {
      if (dataOutputStream != null) {
        dataOutputStream.flush();
        dataOutputStream.close();
      } else if (savedRowMeta != null) {
        initOutputStream(savedRowMeta);
        dataOutputStream.flush();
        dataOutputStream.close();
      }
    } catch (Exception e) {
      log.logError("Error closing spool output stream for transform " + component.getName(), e);
      hasErrors = true;
    }

    try {
      writeManifest(hasErrors || errorOccurred);
    } catch (Exception e) {
      log.logError("Error writing snapshot manifest for transform " + component.getName(), e);
    }
  }

  private void writeManifest(boolean hasErrors) throws Exception {
    String targetFolder =
        ReplayGateUtil.getTransformSpoolPath(variables, gate, pipelineName, component.getName());
    FileObject folderObj = HopVfs.getFileObject(targetFolder, variables);
    if (!folderObj.exists()) {
      folderObj.createFolder();
    }

    SnapshotManifest manifest = new SnapshotManifest();
    manifest.setSnapshotId(UUID.randomUUID().toString());
    manifest.setPipelineName(pipelineName);
    manifest.setTransformName(component.getName());
    manifest.setCopyNr(component.getCopyNr());
    manifest.setCreatedTimestamp(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
    manifest.setRowCount(rowCount.get());
    manifest.setCompression(
        StringUtils.isNotEmpty(gate.getCompression())
            ? gate.getCompression()
            : ReplayDefaults.DEFAULT_COMPRESSION);
    manifest.setDataFile(dataFileName != null ? dataFileName : "data.bin");
    manifest.setStatus(
        hasErrors ? SnapshotManifest.STATUS_INTERRUPTED : SnapshotManifest.STATUS_SEALED);

    if (digest != null) {
      manifest.setChecksum(HexFormat.of().formatHex(digest.digest()));
    }

    if (savedRowMeta != null) {
      for (int i = 0; i < savedRowMeta.size(); i++) {
        IValueMeta vm = savedRowMeta.getValueMeta(i);
        manifest
            .getFields()
            .add(
                new SnapshotFieldMeta(
                    vm.getName(), vm.getTypeDesc(), vm.getLength(), vm.getPrecision()));
      }
    }

    String manifestJson = manifest.toJson();
    String manifestFileName =
        (component.getCopyNr() == 0 && totalCopies <= 1)
            ? "SnapshotManifest.json"
            : ("SnapshotManifest_" + component.getCopyNr() + ".json");
    FileObject manifestFile =
        HopVfs.getFileObject(targetFolder + "/" + manifestFileName, variables);
    try (OutputStream out = HopVfs.getOutputStream(manifestFile, false)) {
      out.write(manifestJson.getBytes(StandardCharsets.UTF_8));
    }
    log.logBasic(
        "Replay Gate ["
            + component.getName()
            + "]: Snapshot manifest written with status "
            + manifest.getStatus()
            + " ("
            + manifest.getRowCount()
            + " rows) to "
            + targetFolder);
  }
}
