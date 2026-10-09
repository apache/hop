/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class WatchFiles extends BaseTransform<WatchFilesMeta, WatchFilesData> {
  private final AtomicBoolean stopping = new AtomicBoolean();

  public WatchFiles(
      TransformMeta transformMeta,
      WatchFilesMeta meta,
      WatchFilesData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    FileWatcher watcher = null;
    FileStateStore store = null;
    try {
      meta.validate(this);
      data.maximumRunMillis = meta.maximumRunMillis(this);
      if (getPipeline().getPipelineType() != PipelineMeta.PipelineType.Normal) {
        throw new IOException("Watch Files requires the normal Local Hop Engine.");
      }
      if (getCopy() != 0 || getTransformMeta().getCopies(this) != 1) {
        throw new IOException("Watch Files requires exactly one transform copy.");
      }
      if (!getPipelineMeta().findPreviousTransforms(getTransformMeta()).isEmpty()) {
        throw new IOException("Watch Files is a source and does not accept input rows.");
      }
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      data.watchId = resolve(meta.getWatchId());
      data.root = VfsFileScanner.resolveFile(resolve(meta.getDirectory()), this);
      if (!data.root.exists() || !data.root.isFolder() || !data.root.isReadable()) {
        throw new IOException("Watch root must be an existing readable directory.");
      }
      if (data.root.isSymbolicLink()) {
        throw new IOException("Watch root must not be a symbolic link.");
      }
      Path localRoot =
          data.root instanceof LocalFile && "file".equals(data.root.getName().getScheme())
              ? VfsFileScanner.localPath(data.root)
              : null;
      if (localRoot != null) {
        if (Files.isSymbolicLink(localRoot)) {
          throw new IOException("Watch root must not be a symbolic link.");
        }
        localRoot = localRoot.toRealPath();
      }
      Path statePath;
      try (FileObject stateFolder =
          VfsFileScanner.resolveFile(resolve(meta.getStateDirectory()), this)) {
        if (!(stateFolder instanceof LocalFile)) {
          throw new IOException(
              "State directory must be local; remote checkpoint stores are not supported.");
        }
        stateFolder.createFolder();
        statePath = VfsFileScanner.localPath(stateFolder).toRealPath();
      }
      if (localRoot != null && statePath.startsWith(localRoot)) {
        throw new IOException("State directory must be outside the watched tree.");
      }
      int maximum = WatchFilesMeta.integer(this, meta.getMaximumEntries(), "Maximum entries");
      int capacity = WatchFilesMeta.integer(this, meta.getEventCapacity(), "Native hint capacity");
      DetectionStrategy strategy = DetectionStrategy.valueOf(meta.getStrategy());
      if (strategy == DetectionStrategy.NATIVE && localRoot == null) {
        throw new IOException(
            "NATIVE requires a local file filesystem. Use AUTO or POLLING for VFS.");
      }
      if (strategy != DetectionStrategy.POLLING && localRoot != null) {
        try {
          watcher =
              new LocalFileWatcher(localRoot, meta.isIncludeSubdirectories(), capacity, maximum);
        } catch (IOException | UnsupportedOperationException e) {
          if (e instanceof WatchLimitException) {
            throw e;
          }
          if (strategy == DetectionStrategy.NATIVE) {
            throw e;
          }
          logBasic("Native watcher unavailable; using VFS polling: " + e.getMessage());
        }
      }
      boolean nativeWatcher = watcher != null;
      if (!nativeWatcher) {
        watcher = new VfsPollingWatcher();
      }
      String include = meta.filenameRegex(this, meta.getIncludeWildcard());
      String exclude = meta.filenameRegex(this, meta.getExcludeWildcard());
      String scope =
          new ObjectMapper()
              .writeValueAsString(
                  new String[] {
                    Boolean.toString(meta.isIncludeSubdirectories()), include, exclude
                  });
      store =
          new JsonFileStateStore(
              statePath, data.watchId, HopVfs.getFriendlyURI(data.root), scope, maximum);
      VfsFileScanner scanner =
          new VfsFileScanner(
              data.root, meta.isIncludeSubdirectories(), include, exclude, maximum, stopping::get);
      java.util.function.Consumer<String> watchLog =
          message -> logBasic("[watch_id=" + data.watchId + "] " + message);
      WatchFilesDiagnostics diagnostics =
          new WatchFilesDiagnostics(
              statePath.resolve(data.watchId).toString(),
              data.watchId,
              WatchFilesClock.SYSTEM,
              WatchFilesMeta.number(
                  this, meta.getSlowOperationThreshold(), 1, "Slow operation threshold"),
              WatchFilesMeta.number(this, meta.getDiagnosticsInterval(), 1, "Diagnostics interval"),
              watchLog);
      data.engine =
          new WatchFilesEngine(
              scanner,
              watcher,
              store,
              new FileStabilityTracker(
                  meta.isWaitUntilStable(),
                  WatchFilesMeta.number(this, meta.getMinimumAge(), 0, "Minimum age"),
                  WatchFilesMeta.integer(this, meta.getStabilityChecks(), "Stability checks"),
                  WatchFilesMeta.number(this, meta.getStabilityInterval(), 1, "Stability interval"),
                  maximum),
              WatchFilesMeta.number(
                  this,
                  nativeWatcher ? meta.getReconciliationInterval() : meta.getPollingInterval(),
                  1,
                  "Scan interval"),
              WatchFilesMeta.number(this, meta.getCheckpointInterval(), 1, "Checkpoint interval"),
              WatchFilesMeta.number(this, meta.getPollingInterval(), 1, "Retry interval"),
              nativeWatcher ? localRoot : null,
              maximum,
              watchLog,
              WatchFilesClock.SYSTEM,
              diagnostics);
      data.engine.initialize(InitialScan.valueOf(meta.getInitialScan()));
      diagnostics.register();
      if (stopping.get()) {
        data.engine.stop();
      }
      logBasic(
          "Watch Files initialized for ["
              + HopVfs.getFriendlyURI(data.root)
              + "], using "
              + (nativeWatcher ? "native WatchService" : "VFS polling")
              + ".");
      return true;
    } catch (Exception e) {
      logError("Unable to initialize Watch Files: " + e.getMessage(), e);
      closeFailedInitialization(watcher, store);
      return false;
    }
  }

  private void closeFailedInitialization(FileWatcher watcher, FileStateStore store) {
    try {
      if (data.engine != null) {
        data.engine.close();
      } else {
        try {
          if (watcher != null) watcher.close();
        } finally {
          if (store != null) store.close();
        }
      }
    } catch (IOException e) {
      logError("Unable to close watcher and release state lock", e);
    }
    data.engine = null;
    try {
      if (data.root != null) {
        data.root.close();
      }
    } catch (IOException e) {
      logError("Unable to close VFS root", e);
    }
    data.root = null;
  }

  @Override
  public boolean processRow() throws HopException {
    if (isStopped() || stopping.get()) {
      setOutputDone();
      return false;
    }
    try {
      if (!data.runStarted) {
        data.runStartedAt = WatchFilesClock.SYSTEM.elapsedMillis();
        data.runStarted = true;
      }
      if (finishIfRuntimeExpired()) return false;
      data.engine.tick();
      if (finishIfRuntimeExpired()) return false;
      FileChangeEvent event = data.engine.next();
      if (finishIfRuntimeExpired()) return false;
      if (event != null) {
        if (meta.enabled(event.getType())) {
          putRow(data.outputRowMeta, toRow(event, data.watchId));
          // putRow can return after a stop without handing the row downstream.
          if (isStopped() || stopping.get()) {
            setOutputDone();
            return false;
          }
          incrementLinesInput();
        }
        data.engine.acknowledge(event, meta.enabled(event.getType()));
        data.engine.checkpoint();
      } else {
        long waitMillis = 100;
        if (data.maximumRunMillis > 0) {
          waitMillis =
              Math.min(
                  waitMillis,
                  Math.max(
                      1,
                      data.maximumRunMillis
                          - (WatchFilesClock.SYSTEM.elapsedMillis() - data.runStartedAt)));
        }
        data.engine.await(waitMillis);
      }
      return !isStopped() && !stopping.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (isStopped() || stopping.get()) {
        setOutputDone();
        return false;
      }
      throw new HopException("Watch Files interrupted.", e);
    } catch (IOException | RuntimeException e) {
      if (isStopped() || stopping.get()) {
        setOutputDone();
        return false;
      }
      throw new HopException("Watch Files failed; acknowledged checkpoint is retained.", e);
    }
  }

  private boolean finishIfRuntimeExpired() throws IOException {
    if (data.maximumRunMillis == 0
        || WatchFilesClock.SYSTEM.elapsedMillis() - data.runStartedAt < data.maximumRunMillis) {
      return false;
    }
    // Close and persist acknowledged observations before signalling normal EOF downstream.
    // Do not mark the pipeline stopped: queued rows must be allowed to finish.
    data.engine.close();
    data.engine = null;
    logBasic("[watch_id=" + data.watchId + "] Maximum run time reached; state saved.");
    setOutputDone();
    return true;
  }

  static Object[] toRow(FileChangeEvent event, String watchId) {
    FileState file = event.file();
    FileState previous = event.getPrevious();
    return new Object[] {
      file.getFilename(),
      file.getShortFilename(),
      file.getPath(),
      file.getUri(),
      event.getType().name(),
      file.getSize(),
      new Date(file.getLastModified()),
      new Date(event.getDetectedAt()),
      false,
      file.getScheme(),
      previous == null ? null : previous.getSize(),
      previous == null ? null : new Date(previous.getLastModified()),
      watchId
    };
  }

  @Override
  public void stopRunning() throws HopException {
    stopping.set(true);
    try {
      WatchFilesEngine engine = data.engine;
      if (engine != null) {
        engine.stop();
      }
    } catch (IOException e) {
      throw new HopException("Unable to stop Watch Files.", e);
    } finally {
      super.stopRunning();
    }
  }

  @Override
  public void dispose() {
    try {
      if (data.engine != null) {
        data.engine.close();
        data.engine = null;
      }
    } catch (IOException e) {
      logError("Unable to checkpoint Watch Files during disposal.", e);
      setErrors(getErrors() + 1);
    } finally {
      try {
        if (data.root != null) {
          data.root.close();
          data.root = null;
        }
      } catch (IOException e) {
        logError("Unable to close VFS root.", e);
      }
      super.dispose();
    }
  }
}
