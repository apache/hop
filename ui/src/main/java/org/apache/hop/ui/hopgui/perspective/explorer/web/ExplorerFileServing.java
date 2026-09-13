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

package org.apache.hop.ui.hopgui.perspective.explorer.web;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.hop.core.util.Utils;

/**
 * Path sandbox, content-type map, and public URL builder for Hop Web explorer file serving. No RAP
 * or SWT types so it can be unit-tested in {@code hop-ui}.
 */
public final class ExplorerFileServing {

  public static final String SERVLET_PATH = "/explorer-file";

  private static final Pattern WINDOWS_DRIVE = Pattern.compile("^[a-zA-Z]:.*");
  private static final Pattern SCHEME_PREFIX = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*:");

  static final Set<String> ALLOWED_EXTENSIONS =
      Set.of(
          "html", "htm", "css", "js", "mjs", "map", "png", "jpg", "jpeg", "gif", "svg", "webp",
          "ico", "bmp", "woff", "woff2", "ttf", "otf", "eot", "pdf");

  private static final Map<String, String> CONTENT_TYPES =
      Map.ofEntries(
          Map.entry("html", "text/html; charset=UTF-8"),
          Map.entry("htm", "text/html; charset=UTF-8"),
          Map.entry("css", "text/css; charset=UTF-8"),
          Map.entry("js", "text/javascript; charset=UTF-8"),
          Map.entry("mjs", "text/javascript; charset=UTF-8"),
          Map.entry("map", "application/json"),
          Map.entry("png", "image/png"),
          Map.entry("jpg", "image/jpeg"),
          Map.entry("jpeg", "image/jpeg"),
          Map.entry("gif", "image/gif"),
          Map.entry("svg", "image/svg+xml"),
          Map.entry("webp", "image/webp"),
          Map.entry("ico", "image/x-icon"),
          Map.entry("bmp", "image/bmp"),
          Map.entry("woff", "font/woff"),
          Map.entry("woff2", "font/woff2"),
          Map.entry("ttf", "font/ttf"),
          Map.entry("otf", "font/otf"),
          Map.entry("eot", "application/vnd.ms-fontobject"),
          Map.entry("pdf", "application/pdf"));

  private ExplorerFileServing() {}

  /**
   * Relative path of {@code file} under {@code root}, or empty when the file is not a descendant.
   */
  public static Optional<String> relativePath(FileObject root, FileObject file)
      throws FileSystemException {
    if (root == null || file == null) {
      return Optional.empty();
    }
    if (!root.getName().isDescendent(file.getName())) {
      return Optional.empty();
    }
    // getRelativeName() keeps '%' URI-escaped; sanitizeRelativePath() expects decoded input.
    return sanitizeRelativePath(UriParser.decode(root.getName().getRelativeName(file.getName())));
  }

  /**
   * Resolve {@code relativePath} under {@code root}. Empty when the path escapes, is a folder, does
   * not exist, or is not an allow-listed extension.
   */
  public static Optional<FileObject> resolveUnderRoot(FileObject root, String relativePath)
      throws FileSystemException {
    if (root == null) {
      return Optional.empty();
    }
    Optional<String> clean = sanitizeRelativePath(relativePath);
    if (clean.isEmpty() || !isAllowedExtension(clean.get())) {
      return Optional.empty();
    }
    FileObject resolved = root.resolveFile(clean.get().replace("%", "%25"));
    if (resolved == null || !root.getName().isDescendent(resolved.getName())) {
      return Optional.empty();
    }
    if (!resolved.exists() || resolved.isFolder()) {
      return Optional.empty();
    }
    return Optional.of(resolved);
  }

  /**
   * Normalize a relative path under the explorer root. Input is treated as already URL-decoded
   * (e.g. from {@code HttpServletRequest.getPathInfo()} or VFS {@code FileName.getRelativeName()}).
   * Rejects absolute paths, schemes, {@code ..} segments, and NUL.
   */
  public static Optional<String> sanitizeRelativePath(String path) {
    if (Utils.isEmpty(path) || path.indexOf('\0') >= 0) {
      return Optional.empty();
    }
    String normalized = path.replace('\\', '/');
    if (normalized.startsWith("/")) {
      return Optional.empty();
    }
    String[] parts = normalized.split("/");
    if (parts.length > 0 && hasSchemeOrDrivePrefix(parts[0])) {
      return Optional.empty();
    }
    List<String> out = new ArrayList<>(parts.length);
    for (String part : parts) {
      if (part.isEmpty() || ".".equals(part)) {
        continue;
      }
      if ("..".equals(part) || "%2e%2e".equalsIgnoreCase(part)) {
        return Optional.empty();
      }
      out.add(part);
    }
    if (out.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(String.join("/", out));
  }

  public static boolean hasSchemeOrDrivePrefix(String firstSegment) {
    if (firstSegment == null || firstSegment.isEmpty()) {
      return false;
    }
    if (WINDOWS_DRIVE.matcher(firstSegment).matches()) {
      return true;
    }
    if (firstSegment.endsWith(":") && SCHEME_PREFIX.matcher(firstSegment).matches()) {
      return true;
    }
    String lower = firstSegment.toLowerCase(Locale.ROOT);
    return lower.startsWith("file:")
        || lower.startsWith("http:")
        || lower.startsWith("https:")
        || lower.startsWith("ftp:");
  }

  /**
   * Resolve a relative href against a document path that is itself under the explorer root. Empty
   * when the result would escape the root.
   */
  public static Optional<String> applyRelative(String documentRelativePath, String href) {
    Optional<String> document = sanitizeRelativePath(documentRelativePath);
    if (document.isEmpty() || Utils.isEmpty(href) || href.indexOf('\0') >= 0) {
      return Optional.empty();
    }
    String decodedHref;
    try {
      decodedHref = URLDecoder.decode(href.replace("+", "%2B"), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
    decodedHref = decodedHref.replace('\\', '/');
    if (decodedHref.startsWith("/")) {
      return Optional.empty();
    }
    String[] hrefParts = decodedHref.split("/");
    if (hrefParts.length > 0 && hasSchemeOrDrivePrefix(hrefParts[0])) {
      return Optional.empty();
    }
    String combined = parentOf(document.get());
    if (combined.isEmpty()) {
      combined = decodedHref;
    } else if (!decodedHref.isEmpty()) {
      combined = combined + "/" + decodedHref;
    }
    List<String> stack = new ArrayList<>();
    for (String part : combined.split("/")) {
      if (part.isEmpty() || ".".equals(part)) {
        continue;
      }
      if ("..".equals(part) || "%2e%2e".equalsIgnoreCase(part)) {
        if (stack.isEmpty()) {
          return Optional.empty();
        }
        stack.remove(stack.size() - 1);
        continue;
      }
      stack.add(part);
    }
    if (stack.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(String.join("/", stack));
  }

  public static boolean isAllowedExtension(String relativePath) {
    return extensionOf(relativePath).filter(ALLOWED_EXTENSIONS::contains).isPresent();
  }

  public static Optional<String> contentType(String relativePath) {
    return extensionOf(relativePath).map(CONTENT_TYPES::get);
  }

  /**
   * Origin-relative public path. Never a scheme-absolute URL. {@code contextPath} is the servlet
   * context ({@code ""} or {@code /hop}); {@code relativePath} is already sanitized.
   */
  public static String buildPublicPath(String contextPath, String token, String relativePath) {
    if (Utils.isEmpty(token) || Utils.isEmpty(relativePath)) {
      throw new IllegalArgumentException("token and relativePath are required");
    }
    String ctx = contextPath == null ? "" : contextPath;
    if ("/".equals(ctx)) {
      ctx = "";
    } else if (ctx.endsWith("/")) {
      ctx = ctx.substring(0, ctx.length() - 1);
    }
    return ctx + SERVLET_PATH + "/" + token + "/" + encodePath(relativePath);
  }

  public static boolean isUuidToken(String token) {
    if (Utils.isEmpty(token)) {
      return false;
    }
    try {
      UUID.fromString(token);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /** Split {@code /{token}/{relative/path}} from a servlet path-info string. */
  public static Optional<PathInfo> parsePathInfo(String pathInfo) {
    if (Utils.isEmpty(pathInfo)) {
      return Optional.empty();
    }
    String value = pathInfo.startsWith("/") ? pathInfo.substring(1) : pathInfo;
    int slash = value.indexOf('/');
    if (slash <= 0 || slash == value.length() - 1) {
      return Optional.empty();
    }
    String token = value.substring(0, slash);
    String relative = value.substring(slash + 1);
    if (!isUuidToken(token)) {
      return Optional.empty();
    }
    Optional<String> clean = sanitizeRelativePath(relative);
    if (clean.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new PathInfo(token, clean.get()));
  }

  public record PathInfo(String token, String relativePath) {}

  private static Optional<String> extensionOf(String relativePath) {
    if (Utils.isEmpty(relativePath)) {
      return Optional.empty();
    }
    int slash = relativePath.lastIndexOf('/');
    String base = slash >= 0 ? relativePath.substring(slash + 1) : relativePath;
    int dot = base.lastIndexOf('.');
    if (dot <= 0 || dot == base.length() - 1) {
      return Optional.empty();
    }
    return Optional.of(base.substring(dot + 1).toLowerCase(Locale.ROOT));
  }

  private static String parentOf(String relativePath) {
    int slash = relativePath.lastIndexOf('/');
    return slash <= 0 ? "" : relativePath.substring(0, slash);
  }

  private static String encodePath(String relativePath) {
    String[] parts = relativePath.split("/");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        sb.append('/');
      }
      sb.append(URLEncoder.encode(parts[i], StandardCharsets.UTF_8).replace("+", "%20"));
    }
    return sb.toString();
  }
}
