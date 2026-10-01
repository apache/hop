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

package org.apache.hop.ui.hopgui;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.client.service.UrlLauncher;
import org.eclipse.rap.rwt.service.ServiceHandler;
import org.eclipse.rap.rwt.service.ServiceManager;
import org.eclipse.rap.rwt.service.UISession;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;

/** Browser-backed file selection and download support for Hop Web. */
final class UserFileTransfer {

  interface UploadListener {
    void uploaded(String filename, Path uploadedFile) throws Exception;

    void error(Exception exception);
  }

  static final long DEFAULT_UPLOAD_TIME_LIMIT = Duration.ofMinutes(5).toMillis();
  static final long DOWNLOAD_TTL_NANOS = Duration.ofMinutes(10).toNanos();
  static final int MAX_PENDING_DOWNLOADS = 16;

  private static final String DOWNLOAD_SERVICE_PREFIX = UserFileTransfer.class.getName() + ".";

  private final Shell shell;
  private final Display display;
  private final ServiceManager serviceManager;
  private final UISession uiSession;
  private final String uiSessionId;
  private final String httpSessionId;
  private final String downloadServiceId = DOWNLOAD_SERVICE_PREFIX + UUID.randomUUID();
  private final Path transferDirectory;
  private final Path uploadDirectory;
  private final Path downloadDirectory;
  private final Map<String, Download> downloads = new ConcurrentHashMap<>();
  private final AtomicBoolean disposed = new AtomicBoolean();
  private final LongSupplier nanoTime;

  UserFileTransfer(Shell shell, Path sessionTempDirectory) throws IOException {
    this(shell, sessionTempDirectory, System::nanoTime);
  }

  UserFileTransfer(Shell shell, Path sessionTempDirectory, LongSupplier nanoTime)
      throws IOException {
    this.shell = shell;
    this.display = shell.getDisplay();
    this.nanoTime = nanoTime;
    serviceManager = RWT.getServiceManager();
    uiSession = RWT.getUISession();
    uiSessionId = uiSession.getId();
    httpSessionId = uiSession.getHttpSession().getId();
    transferDirectory =
        Files.createDirectory(sessionTempDirectory.resolve("transfer-" + UUID.randomUUID()));
    uploadDirectory = Files.createDirectory(transferDirectory.resolve("uploads"));
    downloadDirectory = Files.createDirectory(transferDirectory.resolve("downloads"));
    serviceManager.registerServiceHandler(downloadServiceId, new DownloadServiceHandler());
    uiSession.addUISessionListener(event -> dispose());
  }

  void open(String acceptedExtensions, long uploadSizeLimit, UploadListener listener) {
    if (isDisposed()) {
      listener.error(new IOException("The browser file transfer session is closed."));
      return;
    }

    Path requestDirectory = null;
    try {
      requestDirectory =
          Files.createDirectory(uploadDirectory.resolve(UUID.randomUUID().toString()));
      FileDialog dialog = new FileDialog(shell, SWT.OPEN);
      dialog.setUploadDirectory(requestDirectory.toFile());
      dialog.setUploadSizeLimit(uploadSizeLimit);
      dialog.setUploadTimeLimit(DEFAULT_UPLOAD_TIME_LIMIT);
      if (acceptedExtensions != null && !acceptedExtensions.isBlank()) {
        dialog.setFilterExtensions(acceptedExtensions.split(","));
      }

      String uploadedPath = dialog.open();
      if (uploadedPath == null || uploadedPath.isBlank()) {
        if (!dialog.getExceptions().isEmpty()) {
          throw new IOException(
              "The browser could not upload the selected file.", dialog.getExceptions().get(0));
        }
        return;
      }

      Path uploadedFile = validateUploadedFile(requestDirectory, uploadedPath, uploadSizeLimit);
      listener.uploaded(safeUploadFilename(dialog.getFileName()), uploadedFile);
    } catch (Exception e) {
      listener.error(e);
    } finally {
      deleteTree(requestDirectory);
    }
  }

  void download(String filename, String contentType, byte[] content) throws IOException {
    download(filename, contentType, content, null);
  }

  void download(String filename, String contentType, byte[] content, Runnable onSuccess)
      throws IOException {
    Path file = Files.createTempFile(downloadDirectory, "download-", ".tmp");
    try {
      Files.write(file, content);
      registerDownload(filename, contentType, file, onSuccess);
    } catch (IOException | RuntimeException e) {
      Files.deleteIfExists(file);
      throw e;
    }
  }

  void download(String filename, String contentType, Path source) throws IOException {
    Path file = Files.createTempFile(downloadDirectory, "download-", ".tmp");
    try {
      Files.copy(source, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      registerDownload(filename, contentType, file, null);
    } catch (IOException | RuntimeException e) {
      Files.deleteIfExists(file);
      throw e;
    }
  }

  private synchronized void registerDownload(
      String filename, String contentType, Path file, Runnable onSuccess) throws IOException {
    if (isDisposed()) {
      throw new IOException("The browser file transfer session is closed.");
    }
    removeExpiredDownloads();
    evictOldestDownloadIfNecessary();

    String token = UUID.randomUUID().toString();
    Download download =
        new Download(
            safeHeaderFilename(filename),
            contentType,
            file,
            nanoTime.getAsLong() + DOWNLOAD_TTL_NANOS,
            onSuccess);
    downloads.put(token, download);
    try {
      String url =
          serviceManager.getServiceHandlerUrl(downloadServiceId)
              + "&token="
              + URLEncoder.encode(token, StandardCharsets.UTF_8);
      RWT.getClient().getService(UrlLauncher.class).openURL(url);
    } catch (RuntimeException e) {
      downloads.remove(token, download);
      throw e;
    }
  }

  boolean isDisposed() {
    return disposed.get();
  }

  synchronized void dispose() {
    if (disposed.compareAndSet(false, true)) {
      try {
        serviceManager.unregisterServiceHandler(downloadServiceId);
      } finally {
        downloads.values().forEach(download -> deleteFile(download.file()));
        downloads.clear();
        deleteTree(transferDirectory);
      }
    }
  }

  private void removeExpiredDownloads() {
    long now = nanoTime.getAsLong();
    downloads.forEach(
        (token, download) -> {
          if (download.expiresAtNanos() - now <= 0 && downloads.remove(token, download)) {
            deleteFile(download.file());
          }
        });
  }

  private void evictOldestDownloadIfNecessary() {
    if (downloads.size() < MAX_PENDING_DOWNLOADS) {
      return;
    }
    downloads.entrySet().stream()
        .min(Comparator.comparingLong(entry -> entry.getValue().expiresAtNanos()))
        .ifPresent(
            entry -> {
              if (downloads.remove(entry.getKey(), entry.getValue())) {
                deleteFile(entry.getValue().file());
              }
            });
  }

  private final class DownloadServiceHandler implements ServiceHandler {
    @Override
    public void service(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      if (isDisposed() || !isRequestFromOwningSession(request)) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }

      removeExpiredDownloads();
      String token = request.getParameter("token");
      if (token == null || token.isBlank()) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }

      Download download = downloads.remove(token);
      if (download == null) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }

      try {
        response.setContentType(download.contentType());
        response.setContentLengthLong(Files.size(download.file()));
        response.setHeader("Cache-Control", "private, no-store");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("Content-Disposition", contentDisposition(download.filename()));
        Files.copy(download.file(), response.getOutputStream());
        response.flushBuffer();
        notifyDownloadSucceeded(download.onSuccess());
      } finally {
        deleteFile(download.file());
        if (isDisposed()) {
          // A concurrent session teardown may have encountered this file while it was still open.
          deleteTree(transferDirectory);
        }
      }
    }
  }

  private void notifyDownloadSucceeded(Runnable onSuccess) {
    if (onSuccess == null || isDisposed() || display.isDisposed()) {
      return;
    }
    try {
      display.asyncExec(
          () -> {
            if (!isDisposed() && !shell.isDisposed()) {
              onSuccess.run();
            }
          });
    } catch (SWTException e) {
      if (e.code != SWT.ERROR_DEVICE_DISPOSED) {
        throw e;
      }
    }
  }

  static Path validateUploadedFile(Path requestDirectory, String uploadedPath, long sizeLimit)
      throws IOException {
    Path directory = requestDirectory.toAbsolutePath().normalize();
    Path uploadedFile = Path.of(uploadedPath).toAbsolutePath().normalize();
    if (!uploadedFile.startsWith(directory)
        || !directory.equals(uploadedFile.getParent())
        || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
        || !Files.isRegularFile(uploadedFile, LinkOption.NOFOLLOW_LINKS)
        || Files.size(uploadedFile) > sizeLimit) {
      throw new IOException("The uploaded file is invalid or exceeds the configured size limit.");
    }
    return uploadedFile;
  }

  static String safeUploadFilename(String filename) {
    if (filename == null) {
      return "hop-file";
    }
    String basename = filename.replace('\\', '/');
    basename = basename.substring(basename.lastIndexOf('/') + 1);
    return safeHeaderFilename(basename);
  }

  private boolean isRequestFromOwningSession(HttpServletRequest request) {
    HttpSession requestHttpSession = request.getSession(false);
    if (requestHttpSession == null || !httpSessionId.equals(requestHttpSession.getId())) {
      return false;
    }
    try {
      return uiSessionId.equals(RWT.getUISession().getId());
    } catch (IllegalStateException e) {
      return false;
    }
  }

  static String safeHeaderFilename(String filename) {
    String safe = filename == null ? "hop-file" : filename;
    safe = safe.replaceAll("[\\x00-\\x1f\\x7f\\\\/\"]", "_");
    return safe.isBlank() ? "hop-file" : safe;
  }

  static String contentDisposition(String filename) {
    String ascii = filename.replaceAll("[^\\x20-\\x7E]", "_");
    String encoded = URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");
    return "attachment; filename=\"" + ascii + "\"; filename*=UTF-8''" + encoded;
  }

  private static void deleteFile(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException ignored) {
      // Best effort cleanup; the enclosing session directory is removed on session teardown.
    }
  }

  private static void deleteTree(Path directory) {
    if (directory == null || !Files.exists(directory)) {
      return;
    }
    try (Stream<Path> files = Files.walk(directory)) {
      files.sorted(Comparator.reverseOrder()).forEach(UserFileTransfer::deleteFile);
    } catch (IOException ignored) {
      // Best effort cleanup during request or session teardown.
    }
  }

  private record Download(
      String filename, String contentType, Path file, long expiresAtNanos, Runnable onSuccess) {}
}
