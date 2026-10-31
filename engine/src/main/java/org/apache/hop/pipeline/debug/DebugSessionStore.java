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

package org.apache.hop.pipeline.debug;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.CursorRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.debug.DebugSession.HopState;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Reads and writes a debug session through VFS. {@code session.json} is the description. Each hop
 * folder holds {@code rowmeta.bin} and {@code rows.dat} in Hop's own row format. The folder
 * contains the row data.
 */
public final class DebugSessionStore {

  public static final int FORMAT_VERSION = 1;

  private DebugSessionStore() {}

  /** Default root when the configuration folder is empty: ~/.local/share/hop/debug-sessions. */
  public static String defaultRoot() {
    String home = System.getProperty("user.home", ".");
    return home + "/.local/share/hop/debug-sessions";
  }

  public static String sanitize(String name) {
    if (name == null || name.isBlank()) {
      return "default";
    }
    String cleaned = name.trim().replaceAll("[^A-Za-z0-9._-]", "_");
    if (cleaned.isBlank()) {
      return "default";
    }
    return cleaned.length() > 80 ? cleaned.substring(0, 80) : cleaned;
  }

  public static String sessionDirectory(
      String root, String project, String pipeline, String session) {
    return child(child(child(root, sanitize(project)), sanitize(pipeline)), sanitize(session));
  }

  /** Stable hash of transform names, plugin ids, and hops. A saved session stores this. */
  public static String pipelineHash(PipelineMeta meta) {
    StringBuilder text = new StringBuilder();
    if (meta != null) {
      for (TransformMeta transform : meta.getTransforms()) {
        text.append(transform.getName())
            .append('|')
            .append(transform.getTransformPluginId())
            .append('\n');
      }
      for (PipelineHopMeta hop : meta.getPipelineHops()) {
        String from = hop.getFromTransform() == null ? "" : hop.getFromTransform().getName();
        String to = hop.getToTransform() == null ? "" : hop.getToTransform().getName();
        text.append(from).append("->").append(to).append('\n');
      }
    }
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256")
              .digest(text.toString().getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte value : digest) {
        hex.append(String.format("%02x", value));
      }
      return hex.toString();
    } catch (Exception e) {
      return text.toString();
    }
  }

  public static DebugSession capture(
      Pipeline pipeline,
      String selectedTransform,
      int processCount,
      String project,
      String environment) {
    DebugSession session = new DebugSession();
    PipelineMeta meta = pipeline.getPipelineMeta();
    session.setPipelineFilename(meta == null ? null : meta.getFilename());
    session.setPipelineName(meta == null ? null : meta.getName());
    session.setProject(project);
    session.setEnvironment(environment);
    session.setPipelineHash(pipelineHash(meta));
    session.setSelectedTransform(selectedTransform);
    session.setProcessCount(Math.max(1, processCount));
    session.setSavedAt(System.currentTimeMillis());
    if (pipeline.getDebugController() == null) {
      return session;
    }
    for (CursorRowSet rowSet : pipeline.getDebugController().getCursorRowSets()) {
      HopState hop = new HopState();
      hop.setOrigin(rowSet.getOriginTransformName());
      hop.setOriginCopy(rowSet.getOriginTransformCopy());
      hop.setDestination(rowSet.getDestinationTransformName());
      hop.setDestinationCopy(rowSet.getDestinationTransformCopy());
      hop.setCursor(rowSet.getCursor());
      hop.setRowMeta(rowSet.getRowMeta());
      int stored = (int) Math.min(rowSet.storedCount(), Integer.MAX_VALUE);
      for (int i = 0; i < stored; i++) {
        Object[] row = rowSet.copyStoredRow(i);
        if (row != null) {
          hop.getRows().add(row);
        }
      }
      session.getHops().add(hop);
    }
    return session;
  }

  /** How many saved hops had no matching buffer in the prepared pipeline. */
  public static int apply(Pipeline pipeline, DebugSession session) {
    int skipped = 0;
    if (session == null || pipeline.getDebugController() == null) {
      return skipped;
    }
    List<CursorRowSet> live = pipeline.getDebugController().getCursorRowSets();
    for (HopState hop : session.getHops()) {
      CursorRowSet rowSet = find(live, hop);
      if (rowSet == null) {
        skipped++;
        continue;
      }
      rowSet.clear();
      if (hop.getRowMeta() != null) {
        rowSet.setRowMeta(hop.getRowMeta());
        for (Object[] row : hop.getRows()) {
          rowSet.putRow(hop.getRowMeta(), row);
        }
      }
      int cursor = hop.getCursor();
      if (cursor < 0) {
        cursor = 0;
      }
      if (cursor > rowSet.storedCount()) {
        cursor = (int) rowSet.storedCount();
      }
      rowSet.setCursor(cursor);
    }
    return skipped;
  }

  public static void save(DebugSession session, String directory) throws HopException {
    try {
      FileObject folder = HopVfs.getFileObject(directory);
      folder.createFolder();
      FileDocument document = FileDocument.from(session);
      int index = 0;
      for (HopState hop : session.getHops()) {
        String name =
            String.format(
                "%03d-%s__%s__%d-%d",
                index,
                sanitize(hop.getOrigin()),
                sanitize(hop.getDestination()),
                hop.getOriginCopy(),
                hop.getDestinationCopy());
        document.hops.get(index).folder = name;
        if (hop.getRowMeta() != null) {
          FileObject hopFolder = folder.resolveFile(name);
          hopFolder.createFolder();
          writeRowMeta(child(directory, name + "/rowmeta.bin"), hop.getRowMeta());
          writeRows(child(directory, name + "/rows.dat"), hop.getRowMeta(), hop.getRows());
        }
        index++;
      }
      try (OutputStream out = HopVfs.getOutputStream(child(directory, "session.json"), false)) {
        HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValue(out, document);
      }
    } catch (Exception e) {
      throw new HopException("Unable to save the debug session to '" + directory + "'", e);
    }
  }

  public static DebugSession load(String directory) throws HopException {
    try {
      FileDocument document;
      try (InputStream in = HopVfs.getInputStream(child(directory, "session.json"))) {
        document = HopJson.newMapper().readValue(in, FileDocument.class);
      }
      if (document.formatVersion != FORMAT_VERSION) {
        throw new HopException(
            "Debug session format " + document.formatVersion + " is not supported");
      }
      DebugSession session = document.toSession();
      for (int i = 0; i < document.hops.size(); i++) {
        FileHop fileHop = document.hops.get(i);
        HopState hop = session.getHops().get(i);
        if (fileHop.folder == null) {
          continue;
        }
        String metaPath = child(directory, fileHop.folder + "/rowmeta.bin");
        FileObject metaFile = HopVfs.getFileObject(metaPath);
        if (!metaFile.exists()) {
          continue;
        }
        IRowMeta rowMeta = readRowMeta(metaPath);
        hop.setRowMeta(rowMeta);
        hop.getRows().addAll(readRows(child(directory, fileHop.folder + "/rows.dat"), rowMeta));
      }
      return session;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to load the debug session from '" + directory + "'", e);
    }
  }

  public static List<String> list(String root, String project, String pipeline)
      throws HopException {
    List<String> names = new ArrayList<>();
    try {
      FileObject parent =
          HopVfs.getFileObject(child(child(root, sanitize(project)), sanitize(pipeline)));
      if (!parent.exists() || parent.getType() != FileType.FOLDER) {
        return names;
      }
      for (FileObject child : parent.getChildren()) {
        if (child.getType() == FileType.FOLDER && child.resolveFile("session.json").exists()) {
          names.add(child.getName().getBaseName());
        }
      }
      names.sort(Comparator.naturalOrder());
      return names;
    } catch (Exception e) {
      throw new HopException("Unable to list debug sessions", e);
    }
  }

  public static void delete(String directory) throws HopException {
    try {
      FileObject folder = HopVfs.getFileObject(directory);
      if (folder.exists()) {
        folder.deleteAll();
      }
    } catch (Exception e) {
      throw new HopException("Unable to delete the debug session '" + directory + "'", e);
    }
  }

  /**
   * Appends tab-separated lines to the hop. The hop must already have a row description. Returns
   * how many rows were added.
   */
  public static int importDelimited(CursorRowSet rowSet, String filename) throws HopException {
    if (rowSet.getRowMeta() == null || rowSet.getRowMeta().isEmpty()) {
      throw new HopException("This hop has no row metadata yet");
    }
    IRowMeta rowMeta = rowSet.getRowMeta();
    // convertDataFromString expects the source description to be a string, not the target type.
    IRowMeta convertMeta = rowMeta.cloneToType(IValueMeta.TYPE_STRING);
    int added = 0;
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(HopVfs.getInputStream(filename), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }
        String[] fields = line.split("\t", -1);
        Object[] row = new Object[rowMeta.size()];
        for (int i = 0; i < rowMeta.size(); i++) {
          String text = i < fields.length ? fields[i] : "";
          IValueMeta value = rowMeta.getValueMeta(i);
          row[i] =
              value.convertDataFromString(
                  text, convertMeta.getValueMeta(i), null, null, IValueMeta.TRIM_TYPE_BOTH);
        }
        if (!rowSet.putRow(rowMeta, row)) {
          throw new HopException("Unable to append an imported row");
        }
        added++;
      }
      return added;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to import rows from '" + filename + "'", e);
    }
  }

  private static CursorRowSet find(List<CursorRowSet> live, HopState hop) {
    for (CursorRowSet rowSet : live) {
      if (same(rowSet.getOriginTransformName(), hop.getOrigin())
          && rowSet.getOriginTransformCopy() == hop.getOriginCopy()
          && same(rowSet.getDestinationTransformName(), hop.getDestination())
          && rowSet.getDestinationTransformCopy() == hop.getDestinationCopy()) {
        return rowSet;
      }
    }
    return null;
  }

  private static boolean same(String left, String right) {
    if (left == null) {
      return right == null;
    }
    return left.equals(right);
  }

  private static void writeRowMeta(String path, IRowMeta rowMeta) throws HopFileException {
    try (DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(HopVfs.getOutputStream(path, false)))) {
      rowMeta.writeMeta(out);
    } catch (HopFileException e) {
      throw e;
    } catch (Exception e) {
      throw new HopFileException("Unable to write row metadata to '" + path + "'", e);
    }
  }

  private static IRowMeta readRowMeta(String path) throws HopException {
    try (DataInputStream in =
        new DataInputStream(new BufferedInputStream(HopVfs.getInputStream(path)))) {
      return new RowMeta(in);
    } catch (Exception e) {
      throw new HopException("Unable to read row metadata from '" + path + "'", e);
    }
  }

  private static void writeRows(String path, IRowMeta rowMeta, List<Object[]> rows)
      throws HopFileException {
    try (DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(HopVfs.getOutputStream(path, false)))) {
      out.writeInt(rows.size());
      for (Object[] row : rows) {
        rowMeta.writeData(out, row);
      }
    } catch (HopFileException e) {
      throw e;
    } catch (Exception e) {
      throw new HopFileException("Unable to write rows to '" + path + "'", e);
    }
  }

  private static List<Object[]> readRows(String path, IRowMeta rowMeta) throws HopException {
    List<Object[]> rows = new ArrayList<>();
    try (DataInputStream in =
        new DataInputStream(new BufferedInputStream(HopVfs.getInputStream(path)))) {
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        rows.add(rowMeta.readData(in));
      }
      return rows;
    } catch (Exception e) {
      throw new HopException("Unable to read rows from '" + path + "'", e);
    }
  }

  private static String child(String parent, String name) {
    if (parent == null || parent.isBlank()) {
      return name;
    }
    if (parent.endsWith("/")) {
      return parent + name;
    }
    return parent + "/" + name;
  }

  /** JSON document. Row values stay in the hop folders. */
  @Getter
  @Setter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class FileDocument {
    private int formatVersion = FORMAT_VERSION;
    private String pipelineFilename;
    private String pipelineName;
    private String project;
    private String environment;
    private String pipelineHash;
    private String selectedTransform;
    private int processCount = 1;
    private long savedAt;
    private List<FileHop> hops = new ArrayList<>();

    static FileDocument from(DebugSession session) {
      FileDocument document = new FileDocument();
      document.formatVersion = session.getFormatVersion();
      document.pipelineFilename = session.getPipelineFilename();
      document.pipelineName = session.getPipelineName();
      document.project = session.getProject();
      document.environment = session.getEnvironment();
      document.pipelineHash = session.getPipelineHash();
      document.selectedTransform = session.getSelectedTransform();
      document.processCount = session.getProcessCount();
      document.savedAt = session.getSavedAt();
      for (HopState hop : session.getHops()) {
        FileHop fileHop = new FileHop();
        fileHop.origin = hop.getOrigin();
        fileHop.originCopy = hop.getOriginCopy();
        fileHop.destination = hop.getDestination();
        fileHop.destinationCopy = hop.getDestinationCopy();
        fileHop.cursor = hop.getCursor();
        fileHop.rowCount = hop.getRows().size();
        document.hops.add(fileHop);
      }
      return document;
    }

    DebugSession toSession() {
      DebugSession session = new DebugSession();
      session.setFormatVersion(formatVersion);
      session.setPipelineFilename(pipelineFilename);
      session.setPipelineName(pipelineName);
      session.setProject(project);
      session.setEnvironment(environment);
      session.setPipelineHash(pipelineHash);
      session.setSelectedTransform(selectedTransform);
      session.setProcessCount(processCount);
      session.setSavedAt(savedAt);
      if (hops != null) {
        for (FileHop fileHop : hops) {
          HopState hop = new HopState();
          hop.setOrigin(fileHop.origin);
          hop.setOriginCopy(fileHop.originCopy);
          hop.setDestination(fileHop.destination);
          hop.setDestinationCopy(fileHop.destinationCopy);
          hop.setCursor(fileHop.cursor);
          session.getHops().add(hop);
        }
      }
      return session;
    }
  }

  @Getter
  @Setter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class FileHop {
    private String folder;
    private String origin;
    private int originCopy;
    private String destination;
    private int destinationCopy;
    private int cursor;
    private int rowCount;
  }
}
