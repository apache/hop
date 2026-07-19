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

package org.apache.hop.spark.pkg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Include/exclude rules for {@link SparkProjectPackage#exportProject}. Built-in defaults skip
 * common bulk/IDE dirs ({@code work}, {@code datasets}, {@code target}, …). Project file {@code
 * spark-package.json} and CLI/GUI options merge on top.
 *
 * <p>Not the same as {@code .gitignore}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public final class PackageExportFilter {

  /** Project-home config filename (preferred). */
  public static final String CONFIG_FILENAME = "spark-package.json";

  /** Built-in directory basenames always skipped unless {@link #replaceDefaultExcludeDirs}. */
  public static final Set<String> DEFAULT_EXCLUDE_DIRS =
      Set.of(".git", "datasets", "target", "node_modules", ".idea", ".settings", "work");

  @JsonProperty("excludeDirs")
  private List<String> excludeDirs = new ArrayList<>();

  @JsonProperty("excludeGlobs")
  private List<String> excludeGlobs = new ArrayList<>();

  @JsonProperty("includePaths")
  private List<String> includePaths = new ArrayList<>();

  @JsonProperty("replaceDefaultExcludeDirs")
  private boolean replaceDefaultExcludeDirs;

  public PackageExportFilter() {}

  public PackageExportFilter(
      List<String> excludeDirs,
      List<String> excludeGlobs,
      List<String> includePaths,
      boolean replaceDefaultExcludeDirs) {
    this.excludeDirs = excludeDirs != null ? new ArrayList<>(excludeDirs) : new ArrayList<>();
    this.excludeGlobs = excludeGlobs != null ? new ArrayList<>(excludeGlobs) : new ArrayList<>();
    this.includePaths = includePaths != null ? new ArrayList<>(includePaths) : new ArrayList<>();
    this.replaceDefaultExcludeDirs = replaceDefaultExcludeDirs;
  }

  public static PackageExportFilter empty() {
    return new PackageExportFilter();
  }

  public List<String> getExcludeDirs() {
    return excludeDirs;
  }

  public void setExcludeDirs(List<String> excludeDirs) {
    this.excludeDirs = excludeDirs != null ? excludeDirs : new ArrayList<>();
  }

  public List<String> getExcludeGlobs() {
    return excludeGlobs;
  }

  public void setExcludeGlobs(List<String> excludeGlobs) {
    this.excludeGlobs = excludeGlobs != null ? excludeGlobs : new ArrayList<>();
  }

  public List<String> getIncludePaths() {
    return includePaths;
  }

  public void setIncludePaths(List<String> includePaths) {
    this.includePaths = includePaths != null ? includePaths : new ArrayList<>();
  }

  public boolean isReplaceDefaultExcludeDirs() {
    return replaceDefaultExcludeDirs;
  }

  public void setReplaceDefaultExcludeDirs(boolean replaceDefaultExcludeDirs) {
    this.replaceDefaultExcludeDirs = replaceDefaultExcludeDirs;
  }

  /**
   * Effective directory basenames to skip (lowercase). When {@link #replaceDefaultExcludeDirs} is
   * false, starts from {@link #DEFAULT_EXCLUDE_DIRS} then adds {@link #excludeDirs}.
   */
  @JsonIgnore
  public Set<String> effectiveExcludeDirs() {
    LinkedHashSet<String> dirs = new LinkedHashSet<>();
    if (!replaceDefaultExcludeDirs) {
      for (String d : DEFAULT_EXCLUDE_DIRS) {
        dirs.add(d.toLowerCase(Locale.ROOT));
      }
    }
    for (String d : excludeDirs) {
      if (StringUtils.isNotBlank(d)) {
        dirs.add(d.trim().toLowerCase(Locale.ROOT));
      }
    }
    return Collections.unmodifiableSet(dirs);
  }

  /**
   * Merge another filter on top of this one. Lists append; if {@code
   * other.replaceDefaultExcludeDirs} is true, that flag is set and default dirs are not used when
   * computing {@link #effectiveExcludeDirs()} after merge (other's excludeDirs replace the combined
   * list for the replace semantics: we set replace flag and use only other's dirs + this's dirs if
   * needed).
   *
   * <p>Merge order intended: {@code defaults-as-empty.merge(file).merge(cli)}.
   */
  public PackageExportFilter merge(PackageExportFilter other) {
    if (other == null) {
      return copy();
    }
    PackageExportFilter out = copy();
    if (other.replaceDefaultExcludeDirs) {
      out.replaceDefaultExcludeDirs = true;
      out.excludeDirs = new ArrayList<>(other.excludeDirs);
    } else {
      LinkedHashSet<String> dirs = new LinkedHashSet<>(out.excludeDirs);
      dirs.addAll(other.excludeDirs);
      out.excludeDirs = new ArrayList<>(dirs);
    }
    LinkedHashSet<String> globs = new LinkedHashSet<>(out.excludeGlobs);
    globs.addAll(other.excludeGlobs);
    out.excludeGlobs = new ArrayList<>(globs);
    LinkedHashSet<String> includes = new LinkedHashSet<>(out.includePaths);
    includes.addAll(other.includePaths);
    out.includePaths = new ArrayList<>(includes);
    return out;
  }

  public PackageExportFilter copy() {
    return new PackageExportFilter(
        excludeDirs, excludeGlobs, includePaths, replaceDefaultExcludeDirs);
  }

  /**
   * Whether a project-relative path should be excluded from the package.
   *
   * @param relativePath path using {@code /} separators, relative to project home
   */
  public boolean shouldSkipRelative(String relativePath) {
    if (StringUtils.isEmpty(relativePath)) {
      return true;
    }
    String rel = relativePath.replace('\\', '/');
    while (rel.startsWith("./")) {
      rel = rel.substring(2);
    }
    if (isIncluded(rel)) {
      return false;
    }
    String[] parts = rel.split("/");
    Set<String> skipDirs = effectiveExcludeDirs();
    for (String part : parts) {
      if (part.startsWith(".") && part.length() > 1) {
        return true;
      }
      if (skipDirs.contains(part.toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    for (String pattern : excludeGlobs) {
      if (StringUtils.isBlank(pattern)) {
        continue;
      }
      try {
        PathMatcher matcher =
            FileSystems.getDefault().getPathMatcher("glob:" + pattern.trim().replace('\\', '/'));
        // PathMatcher needs a Path; use relative path as a pseudo-path
        if (matcher.matches(java.nio.file.Path.of(rel))) {
          return true;
        }
      } catch (Exception ignored) {
        // invalid glob: treat as no match
      }
    }
    return false;
  }

  /** True if folder basename should not be traversed (unless an include lives under it). */
  public boolean shouldSkipDirectoryBasename(String basename, int depth) {
    if (StringUtils.isEmpty(basename)) {
      return false;
    }
    if (basename.startsWith(".") && depth > 0) {
      return true;
    }
    if (effectiveExcludeDirs().contains(basename.toLowerCase(Locale.ROOT))) {
      // Still traverse if any includePaths live under this directory name as a segment
      for (String inc : includePaths) {
        if (StringUtils.isBlank(inc)) {
          continue;
        }
        String n = inc.replace('\\', '/');
        if (n.equals(basename)
            || n.startsWith(basename + "/")
            || n.contains("/" + basename + "/")) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private boolean isIncluded(String rel) {
    for (String raw : includePaths) {
      if (StringUtils.isBlank(raw)) {
        continue;
      }
      String inc = raw.trim().replace('\\', '/');
      while (inc.startsWith("./")) {
        inc = inc.substring(2);
      }
      if (rel.equals(inc) || rel.startsWith(inc.endsWith("/") ? inc : inc + "/")) {
        return true;
      }
    }
    return false;
  }

  /** Load {@link #CONFIG_FILENAME} from project home, or empty filter if missing. */
  public static PackageExportFilter loadFromProjectHome(String projectHome) throws HopException {
    if (StringUtils.isEmpty(projectHome)) {
      return empty();
    }
    String path = projectHome;
    if (!path.endsWith("/") && !path.endsWith("\\")) {
      path = path + "/";
    }
    path = path + CONFIG_FILENAME;
    try {
      FileObject fo = HopVfs.getFileObject(path);
      if (!fo.exists() || fo.getType() != FileType.FILE) {
        return empty();
      }
      try (InputStream in = HopVfs.getInputStream(fo)) {
        ObjectMapper mapper = HopJson.newMapper();
        PackageExportFilter f = mapper.readValue(in, PackageExportFilter.class);
        return f != null ? f : empty();
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to read Spark package filter config '" + path + "': " + e.getMessage(), e);
    }
  }

  /** Write this filter to {@code projectHome/spark-package.json}. */
  public static void saveToProjectHome(String projectHome, PackageExportFilter filter)
      throws HopException {
    if (StringUtils.isEmpty(projectHome)) {
      throw new HopException("Project home is required to save " + CONFIG_FILENAME);
    }
    if (filter == null) {
      filter = empty();
    }
    String path = projectHome;
    if (!path.endsWith("/") && !path.endsWith("\\")) {
      path = path + "/";
    }
    path = path + CONFIG_FILENAME;
    try {
      FileObject fo = HopVfs.getFileObject(path);
      ObjectMapper mapper = HopJson.newMapper();
      mapper.enable(SerializationFeature.INDENT_OUTPUT);
      try (OutputStream out = HopVfs.getOutputStream(fo, false)) {
        mapper.writeValue(out, filter);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to write Spark package filter config '" + path + "': " + e.getMessage(), e);
    }
  }

  /** Parse comma-separated list for CLI. */
  public static List<String> splitCsv(String csv) {
    if (StringUtils.isBlank(csv)) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (String p : csv.split(",")) {
      if (StringUtils.isNotBlank(p)) {
        out.add(p.trim());
      }
    }
    return out;
  }
}
