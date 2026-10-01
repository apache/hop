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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.client.Client;
import org.eclipse.rap.rwt.client.service.UrlLauncher;
import org.eclipse.rap.rwt.service.ServiceHandler;
import org.eclipse.rap.rwt.service.ServiceManager;
import org.eclipse.rap.rwt.service.UISession;
import org.eclipse.rap.rwt.service.UISessionEvent;
import org.eclipse.rap.rwt.service.UISessionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class UserFileTransferTest {

  private static final byte[] CONTENT = "<pipeline/>".getBytes(StandardCharsets.UTF_8);

  @TempDir Path tempDirectory;

  private final AtomicLong nanoTime = new AtomicLong();
  private final List<String> urls = new ArrayList<>();
  private final Queue<Runnable> uiCallbacks = new ArrayDeque<>();

  private MockedStatic<RWT> rwt;
  private UserFileTransfer transfer;
  private Shell shell;
  private Display display;
  private ServiceManager serviceManager;
  private UISession uiSession;
  private HttpSession httpSession;
  private UrlLauncher launcher;
  private ServiceHandler handler;
  private String serviceId;

  @BeforeEach
  void setUp() throws Exception {
    shell = mock(Shell.class);
    display = mock(Display.class);
    serviceManager = mock(ServiceManager.class);
    uiSession = mock(UISession.class);
    httpSession = mock(HttpSession.class);
    Client client = mock(Client.class);
    launcher = mock(UrlLauncher.class);
    when(shell.getDisplay()).thenReturn(display);
    when(uiSession.getId()).thenReturn("owning-ui-session");
    when(uiSession.getHttpSession()).thenReturn(httpSession);
    when(httpSession.getId()).thenReturn("owning-http-session");
    when(client.getService(UrlLauncher.class)).thenReturn(launcher);
    when(serviceManager.getServiceHandlerUrl(anyString())).thenReturn("/hop?service=download");
    doAnswer(
            invocation -> {
              urls.add(invocation.getArgument(0));
              return null;
            })
        .when(launcher)
        .openURL(anyString());
    doAnswer(
            invocation -> {
              uiCallbacks.add(invocation.getArgument(0));
              return null;
            })
        .when(display)
        .asyncExec(any(Runnable.class));

    rwt = mockStatic(RWT.class);
    rwt.when(RWT::getServiceManager).thenReturn(serviceManager);
    rwt.when(RWT::getUISession).thenReturn(uiSession);
    rwt.when(RWT::getClient).thenReturn(client);
    transfer = new UserFileTransfer(shell, tempDirectory, nanoTime::get);
    ArgumentCaptor<ServiceHandler> handlerCaptor = ArgumentCaptor.forClass(ServiceHandler.class);
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    verify(serviceManager).registerServiceHandler(idCaptor.capture(), handlerCaptor.capture());
    handler = handlerCaptor.getValue();
    serviceId = idCaptor.getValue();
  }

  @AfterEach
  void tearDown() {
    try {
      if (transfer != null && !transfer.isDisposed()) {
        transfer.dispose();
      }
    } finally {
      if (rwt != null) {
        rwt.close();
      }
    }
  }

  @Test
  void servesContentWithSafeHeadersAndConsumesTheTokenOnce() throws Exception {
    String token = download("r\u00e9sum\u00e9\r\n\".hpl", null);
    HttpServletResponse response = mock(HttpServletResponse.class);
    TestOutputStream output = new TestOutputStream();
    when(response.getOutputStream()).thenReturn(output);

    handler.service(request(token), response);

    assertArrayEquals(CONTENT, output.bytes.toByteArray());
    verify(response).setContentType("application/xml");
    verify(response).setContentLengthLong(CONTENT.length);
    verify(response).setHeader("Cache-Control", "private, no-store");
    verify(response).setHeader("Pragma", "no-cache");
    verify(response).setHeader("X-Content-Type-Options", "nosniff");
    verify(response)
        .setHeader(
            "Content-Disposition",
            "attachment; filename=\"r_sum____.hpl\"; filename*=UTF-8''r%C3%A9sum%C3%A9___.hpl");
    verify(response).flushBuffer();
    assertEquals(0, pendingFileCount());
    assertNotFound(token);
  }

  @Test
  void rejectsAnotherHttpSessionWithoutConsumingTheToken() throws Exception {
    String token = download("pipeline.hpl", null);
    HttpSession foreign = mock(HttpSession.class);
    when(foreign.getId()).thenReturn("foreign-http-session");
    HttpServletRequest request = request(token);
    when(request.getSession(false)).thenReturn(foreign);
    HttpServletResponse response = mock(HttpServletResponse.class);

    handler.service(request, response);

    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
    assertEquals(1, pendingFileCount());
    assertDownloads(token);
  }

  @Test
  void rejectsAnotherUiSessionWithinTheSameHttpSession() throws Exception {
    String token = download("pipeline.hpl", null);
    UISession foreign = mock(UISession.class);
    when(foreign.getId()).thenReturn("foreign-ui-session");
    rwt.when(RWT::getUISession).thenReturn(foreign);

    assertNotFound(token);

    assertEquals(1, pendingFileCount());
    rwt.when(RWT::getUISession).thenReturn(uiSession);
    assertDownloads(token);
  }

  @Test
  void rejectsRequestsWithoutAnHttpSession() throws Exception {
    String token = download("pipeline.hpl", null);
    HttpServletRequest request = request(token);
    when(request.getSession(false)).thenReturn(null);
    HttpServletResponse response = mock(HttpServletResponse.class);

    handler.service(request, response);

    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
    verify(request, never()).getSession(true);
    assertEquals(1, pendingFileCount());
  }

  @Test
  void rejectsRequestsWithoutAnActiveUiSession() throws Exception {
    String token = download("pipeline.hpl", null);
    rwt.when(RWT::getUISession).thenThrow(new IllegalStateException("Session is gone"));

    assertNotFound(token);

    assertEquals(1, pendingFileCount());
  }

  @Test
  void rejectsMissingBlankAndUnknownTokens() throws Exception {
    download("pipeline.hpl", null);

    assertNotFound(null);
    assertNotFound(" ");
    assertNotFound("unknown-token");

    assertEquals(1, pendingFileCount());
  }

  @Test
  void expiresAndDeletesPendingDownloadsAtTheTtlBoundary() throws Exception {
    Runnable callback = mock(Runnable.class);
    String token = download("pipeline.hpl", callback);
    nanoTime.set(UserFileTransfer.DOWNLOAD_TTL_NANOS);

    assertNotFound(token);

    assertEquals(0, pendingFileCount());
    verify(callback, never()).run();
    assertTrue(uiCallbacks.isEmpty());
  }

  @Test
  void registeringADownloadAlsoRemovesExpiredFiles() throws Exception {
    download("expired.hpl", null);
    nanoTime.set(UserFileTransfer.DOWNLOAD_TTL_NANOS);

    String current = download("current.hpl", null);

    assertEquals(1, pendingFileCount());
    assertDownloads(current);
  }

  @Test
  void evictsTheOldestDownloadAboveThePendingLimit() throws Exception {
    List<String> tokens = new ArrayList<>();
    for (int index = 0; index <= UserFileTransfer.MAX_PENDING_DOWNLOADS; index++) {
      nanoTime.incrementAndGet();
      tokens.add(download("pipeline-" + index + ".hpl", null));
    }

    assertEquals(UserFileTransfer.MAX_PENDING_DOWNLOADS, pendingFileCount());
    assertNotFound(tokens.getFirst());
    for (String token : tokens.subList(1, tokens.size())) {
      assertDownloads(token);
    }
    assertEquals(0, pendingFileCount());
  }

  @Test
  void notifiesSuccessOnlyOnTheUiThreadAfterTheResponseHasFlushed() throws Exception {
    Runnable callback = mock(Runnable.class);
    String token = download("pipeline.hpl", callback);
    verify(callback, never()).run();
    assertTrue(uiCallbacks.isEmpty());

    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream()).thenReturn(new TestOutputStream());
    doAnswer(
            invocation -> {
              assertTrue(uiCallbacks.isEmpty());
              return null;
            })
        .when(response)
        .flushBuffer();
    handler.service(request(token), response);

    verify(callback, never()).run();
    assertEquals(1, uiCallbacks.size());
    uiCallbacks.remove().run();
    verify(callback).run();
  }

  @Test
  void failedStreamingDeletesTheFileWithoutNotifyingSuccess() throws Exception {
    Runnable callback = mock(Runnable.class);
    String token = download("pipeline.hpl", callback);
    HttpServletResponse response = mock(HttpServletResponse.class);
    TestOutputStream output = new TestOutputStream();
    output.failWrite = true;
    when(response.getOutputStream()).thenReturn(output);

    assertThrows(IOException.class, () -> handler.service(request(token), response));

    assertEquals(0, pendingFileCount());
    assertNotFound(token);
    verify(response, never()).flushBuffer();
    verify(callback, never()).run();
    assertTrue(uiCallbacks.isEmpty());
  }

  @Test
  void failedFlushDoesNotNotifySuccess() throws Exception {
    Runnable callback = mock(Runnable.class);
    String token = download("pipeline.hpl", callback);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream()).thenReturn(new TestOutputStream());
    doThrow(new IOException("Connection closed")).when(response).flushBuffer();

    assertThrows(IOException.class, () -> handler.service(request(token), response));

    assertEquals(0, pendingFileCount());
    verify(callback, never()).run();
    assertTrue(uiCallbacks.isEmpty());
  }

  @Test
  void discardsQueuedCallbacksAfterSessionDisposal() throws Exception {
    Runnable callback = mock(Runnable.class);
    assertDownloads(download("pipeline.hpl", callback));

    transfer.dispose();
    uiCallbacks.remove().run();

    verify(callback, never()).run();
  }

  @Test
  void discardsQueuedCallbacksWhenTheShellCloses() throws Exception {
    Runnable callback = mock(Runnable.class);
    assertDownloads(download("pipeline.hpl", callback));

    when(shell.isDisposed()).thenReturn(true);
    uiCallbacks.remove().run();

    verify(callback, never()).run();
  }

  @Test
  void toleratesDisplayDisposalWhileSchedulingSuccess() throws Exception {
    Runnable callback = mock(Runnable.class);
    doThrow(new SWTException(SWT.ERROR_DEVICE_DISPOSED))
        .when(display)
        .asyncExec(any(Runnable.class));

    assertDownloads(download("pipeline.hpl", callback));

    verify(callback, never()).run();
    assertEquals(0, pendingFileCount());
  }

  @Test
  void cleansUpWhenTheBrowserCannotLaunchTheDownload() throws Exception {
    doThrow(new IllegalStateException("Client is unavailable")).when(launcher).openURL(anyString());

    assertThrows(
        IllegalStateException.class,
        () -> transfer.download("pipeline.hpl", "application/xml", CONTENT));

    assertEquals(0, pendingFileCount());
  }

  @Test
  void cleansUpWhenCreatingTheDownloadUrlFails() throws Exception {
    when(serviceManager.getServiceHandlerUrl(anyString()))
        .thenThrow(new IllegalStateException("Application context is closed"));

    assertThrows(
        IllegalStateException.class,
        () -> transfer.download("pipeline.hpl", "application/xml", CONTENT));

    assertEquals(0, pendingFileCount());
    assertTrue(urls.isEmpty());
  }

  @Test
  void sessionDisposalDuringStreamingSuppressesSuccessAndCleansFiles() throws Exception {
    Runnable callback = mock(Runnable.class);
    String token = download("pipeline.hpl", callback);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream()).thenReturn(new TestOutputStream());
    doAnswer(
            invocation -> {
              transfer.dispose();
              return null;
            })
        .when(response)
        .flushBuffer();

    handler.service(request(token), response);

    verify(callback, never()).run();
    assertTrue(uiCallbacks.isEmpty());
    try (var paths = Files.list(tempDirectory)) {
      assertEquals(0, paths.count());
    }
  }

  @Test
  void cleansUpWhenCopyingADownloadSourceFails() throws Exception {
    assertThrows(
        IOException.class,
        () ->
            transfer.download("pipeline.hpl", "application/xml", tempDirectory.resolve("missing")));

    assertEquals(0, pendingFileCount());
    assertTrue(urls.isEmpty());
  }

  @Test
  void sessionDestructionUnregistersTheServiceAndRemovesAllFiles() throws Exception {
    String token = download("pipeline.hpl", null);
    ArgumentCaptor<UISessionListener> listener = ArgumentCaptor.forClass(UISessionListener.class);
    verify(uiSession).addUISessionListener(listener.capture());

    listener.getValue().beforeDestroy(new UISessionEvent(uiSession));
    transfer.dispose();

    assertTrue(transfer.isDisposed());
    verify(serviceManager, times(1)).unregisterServiceHandler(serviceId);
    try (var paths = Files.list(tempDirectory)) {
      assertEquals(0, paths.count());
    }
    assertNotFound(token);
    assertThrows(
        IOException.class, () -> transfer.download("pipeline.hpl", "application/xml", CONTENT));
  }

  @Test
  void stillCleansFilesIfUnregisteringTheServiceFails() throws Exception {
    download("pipeline.hpl", null);
    doThrow(new IllegalStateException("Application context is closed"))
        .when(serviceManager)
        .unregisterServiceHandler(serviceId);

    assertThrows(IllegalStateException.class, transfer::dispose);

    assertTrue(transfer.isDisposed());
    try (var paths = Files.list(tempDirectory)) {
      assertEquals(0, paths.count());
    }
  }

  @Test
  void acceptsOnlyARegularDirectChildWithinTheSizeLimit() throws Exception {
    Path directory = Files.createDirectory(tempDirectory.resolve("request"));
    Path file = Files.write(directory.resolve("pipeline.hpl"), CONTENT);

    assertEquals(
        file.toAbsolutePath().normalize(),
        UserFileTransfer.validateUploadedFile(directory, file.toString(), CONTENT.length));
    assertThrows(
        IOException.class,
        () ->
            UserFileTransfer.validateUploadedFile(directory, file.toString(), CONTENT.length - 1));
    assertThrows(
        IOException.class,
        () ->
            UserFileTransfer.validateUploadedFile(directory, directory.toString(), CONTENT.length));
  }

  @Test
  void rejectsOutsideNestedAndTraversalUploadPathsWithoutDeletingThem() throws Exception {
    Path directory = Files.createDirectory(tempDirectory.resolve("request"));
    Path outside = Files.write(tempDirectory.resolve("outside.hpl"), CONTENT);
    Path nestedDirectory = Files.createDirectory(directory.resolve("nested"));
    Path nested = Files.write(nestedDirectory.resolve("pipeline.hpl"), CONTENT);
    Path siblingDirectory = Files.createDirectory(tempDirectory.resolve("request-sibling"));
    Path sibling = Files.write(siblingDirectory.resolve("pipeline.hpl"), CONTENT);
    for (Path file : List.of(outside, nested, sibling, directory.resolve("../outside.hpl"))) {
      assertThrows(
          IOException.class,
          () -> UserFileTransfer.validateUploadedFile(directory, file.toString(), CONTENT.length));
    }

    assertArrayEquals(CONTENT, Files.readAllBytes(outside));
    assertArrayEquals(CONTENT, Files.readAllBytes(nested));
    assertArrayEquals(CONTENT, Files.readAllBytes(sibling));
  }

  @Test
  void rejectsSymbolicLinkUploads() throws Exception {
    Path directory = Files.createDirectory(tempDirectory.resolve("request"));
    Path outside = Files.write(tempDirectory.resolve("outside.hpl"), CONTENT);
    Path link = directory.resolve("pipeline.hpl");
    try {
      Files.createSymbolicLink(link, outside);
    } catch (IOException | UnsupportedOperationException | SecurityException e) {
      assumeTrue(false, "Symbolic link creation is unavailable: " + e.getClass().getSimpleName());
    }

    assertThrows(
        IOException.class,
        () -> UserFileTransfer.validateUploadedFile(directory, link.toString(), CONTENT.length));
    assertArrayEquals(CONTENT, Files.readAllBytes(outside));
  }

  @Test
  void usesASanitizedBasenameForUploadedFiles() {
    assertEquals("pipeline.hpl", UserFileTransfer.safeUploadFilename("C:\\fakepath\\pipeline.hpl"));
    assertEquals("pipeline.hpl", UserFileTransfer.safeUploadFilename("../../pipeline.hpl"));
    assertEquals("pipeline__.hpl", UserFileTransfer.safeUploadFilename("pipeline\r\n.hpl"));
    assertEquals("hop-file", UserFileTransfer.safeUploadFilename(null));
    assertEquals("hop-file", UserFileTransfer.safeUploadFilename("/"));
  }

  @Test
  void rejectsAndCleansAnOutsideUploadWithoutDeletingTheForeignFile() throws Exception {
    Path outside = Files.write(tempDirectory.resolve("outside.hpl"), CONTENT);
    UserFileTransfer.UploadListener listener = mock(UserFileTransfer.UploadListener.class);
    try (MockedConstruction<FileDialog> dialogs =
        mockConstruction(
            FileDialog.class,
            (dialog, context) -> when(dialog.open()).thenReturn(outside.toString()))) {
      transfer.open("*.hpl", CONTENT.length, listener);

      verify(listener).error(any(IOException.class));
      verify(listener, never()).uploaded(anyString(), any(Path.class));
      ArgumentCaptor<java.io.File> directory = ArgumentCaptor.forClass(java.io.File.class);
      verify(dialogs.constructed().getFirst()).setUploadDirectory(directory.capture());
      assertFalse(Files.exists(directory.getValue().toPath()));
    }
    assertArrayEquals(CONTENT, Files.readAllBytes(outside));
  }

  @Test
  void cancellingAnUploadCleansItsRequestDirectory() throws Exception {
    UserFileTransfer.UploadListener listener = mock(UserFileTransfer.UploadListener.class);
    try (MockedConstruction<FileDialog> dialogs = mockConstruction(FileDialog.class)) {
      transfer.open("*.hpl", CONTENT.length, listener);

      verify(listener, never()).error(any(Exception.class));
      verify(listener, never()).uploaded(anyString(), any(Path.class));
      ArgumentCaptor<java.io.File> directory = ArgumentCaptor.forClass(java.io.File.class);
      verify(dialogs.constructed().getFirst()).setUploadDirectory(directory.capture());
      assertFalse(Files.exists(directory.getValue().toPath()));
    }
  }

  @Test
  void cleansTheUploadedFileEvenWhenTheListenerFails() throws Exception {
    UserFileTransfer.UploadListener listener = mock(UserFileTransfer.UploadListener.class);
    doThrow(new IOException("Invalid pipeline"))
        .when(listener)
        .uploaded(anyString(), any(Path.class));
    List<Path> uploadDirectories = new ArrayList<>();
    try (MockedConstruction<FileDialog> dialogs =
        mockConstruction(
            FileDialog.class,
            (dialog, context) -> {
              doAnswer(
                      invocation -> {
                        uploadDirectories.add(((java.io.File) invocation.getArgument(0)).toPath());
                        return null;
                      })
                  .when(dialog)
                  .setUploadDirectory(any(java.io.File.class));
              when(dialog.open())
                  .thenAnswer(
                      invocation ->
                          Files.write(uploadDirectories.getFirst().resolve("pipeline.hpl"), CONTENT)
                              .toString());
              when(dialog.getFileName()).thenReturn("C:\\fakepath\\pipeline.hpl");
            })) {
      transfer.open("*.hpl", CONTENT.length, listener);

      assertEquals(1, dialogs.constructed().size());
      verify(listener)
          .uploaded("pipeline.hpl", uploadDirectories.getFirst().resolve("pipeline.hpl"));
      verify(listener).error(any(IOException.class));
    }
    assertFalse(Files.exists(uploadDirectories.getFirst()));
  }

  private String download(String filename, Runnable onSuccess) throws IOException {
    transfer.download(filename, "application/xml", CONTENT, onSuccess);
    String url = urls.getLast();
    return url.substring(url.indexOf("&token=") + "&token=".length());
  }

  private HttpServletRequest request(String token) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getSession(false)).thenReturn(httpSession);
    when(request.getParameter("token")).thenReturn(token);
    return request;
  }

  private void assertNotFound(String token) throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    handler.service(request(token), response);
    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
    verify(response, never()).getOutputStream();
  }

  private void assertDownloads(String token) throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    TestOutputStream output = new TestOutputStream();
    when(response.getOutputStream()).thenReturn(output);
    handler.service(request(token), response);
    assertArrayEquals(CONTENT, output.bytes.toByteArray());
    verify(response).flushBuffer();
  }

  private long pendingFileCount() throws IOException {
    try (var files = Files.walk(tempDirectory)) {
      return files.filter(Files::isRegularFile).count();
    }
  }

  private static class TestOutputStream extends ServletOutputStream {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private boolean failWrite;

    @Override
    public void write(int value) throws IOException {
      if (failWrite) {
        throw new IOException("Connection closed");
      }
      bytes.write(value);
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setWriteListener(WriteListener listener) {}
  }
}
