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

package org.apache.hop.lineage.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;

/**
 * Builds a shallow structural tree from tabular column locators so sinks can show nesting without
 * parsing the file.
 */
public final class FileIoSchemaTrees {

  private FileIoSchemaTrees() {}

  /**
   * Merges {@link FileIoTabularColumn} {@code sourcePath} values into a forest of {@link
   * FileIoSchemaNode}. Columns without paths are skipped. Syntax controls path splitting (XPath
   * uses {@code /}; JSON and YAML use {@code .} with a leading {@code $} stripped).
   */
  public static List<FileIoSchemaNode> mergePaths(List<FileIoTabularColumn> columns) {
    if (columns == null || columns.isEmpty()) {
      return List.of();
    }
    Map<String, TrieNode> roots = new LinkedHashMap<>();
    for (FileIoTabularColumn col : columns) {
      if (col == null || Utils.isEmpty(col.getSourcePath())) {
        continue;
      }
      String[] segments = splitPath(col.getSourcePath(), col.getPathSyntax());
      if (segments.length == 0) {
        continue;
      }
      insertUnder(roots, segments, 0, "");
    }
    List<FileIoSchemaNode> out = new ArrayList<>();
    for (TrieNode n : roots.values()) {
      out.add(toNode(n));
    }
    return List.copyOf(out);
  }

  private static void insertUnder(
      Map<String, TrieNode> level, String[] segments, int depth, String parentPath) {
    if (depth >= segments.length) {
      return;
    }
    String seg = segments[depth];
    String full = parentPath.isEmpty() ? seg : parentPath + "/" + seg;
    TrieNode child = level.get(seg);
    if (child == null) {
      child = new TrieNode(seg, full);
      level.put(seg, child);
    } else if (child.fullPath.length() < full.length()) {
      child.fullPath = full;
    }
    if (depth + 1 < segments.length) {
      insertUnder(child.children, segments, depth + 1, full);
    }
  }

  private static FileIoSchemaNode toNode(TrieNode n) {
    String kind = n.children.isEmpty() ? "value" : "object";
    List<FileIoSchemaNode> kids = new ArrayList<>();
    for (TrieNode c : n.children.values()) {
      kids.add(toNode(c));
    }
    return new FileIoSchemaNode(n.segment, n.fullPath, kind, kids);
  }

  private static String[] splitPath(String path, FileIoPathSyntax syntax) {
    String p = path.trim();
    if (p.isEmpty()) {
      return new String[0];
    }
    if (syntax == FileIoPathSyntax.XPATH) {
      String[] raw = p.split("/");
      List<String> parts = new ArrayList<>();
      for (String r : raw) {
        if (!Utils.isEmpty(r)) {
          parts.add(r);
        }
      }
      return parts.toArray(new String[0]);
    }
    if (p.startsWith("$.")) {
      p = p.substring(2);
    } else if ("$".equals(p)) {
      return new String[0];
    }
    List<String> parts = new ArrayList<>();
    for (String s : p.split("\\.")) {
      if (!Utils.isEmpty(s)) {
        parts.add(s);
      }
    }
    return parts.toArray(new String[0]);
  }

  private static final class TrieNode {
    final String segment;
    String fullPath;
    final Map<String, TrieNode> children = new LinkedHashMap<>();

    TrieNode(String segment, String fullPath) {
      this.segment = segment;
      this.fullPath = fullPath;
    }
  }
}
