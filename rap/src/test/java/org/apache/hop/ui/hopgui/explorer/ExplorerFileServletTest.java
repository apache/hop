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

package org.apache.hop.ui.hopgui.explorer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.perspective.explorer.web.ExplorerFileServing;
import org.eclipse.rap.rwt.service.UISession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExplorerFileServletTest {

  private static final byte[] HTML =
      "<html><body>Hello</body></html>".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CSS = "body{color:red}".getBytes(StandardCharsets.UTF_8);

  private ExplorerFileServlet servlet;
  private UISession uiSession;
  private HttpSession httpSession;
  private String ramRoot;
  private ExplorerFileLease lease;

  @BeforeEach
  void setUp() throws Exception {
    ExplorerFileRegistry.clear();
    servlet = new ExplorerFileServlet();
    uiSession = mock(UISession.class);
    httpSession = mock(HttpSession.class);
    when(uiSession.getId()).thenReturn("ui-session");
    when(uiSession.getHttpSession()).thenReturn(httpSession);
    when(httpSession.getId()).thenReturn("http-session");
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Runnable.class).run();
              return null;
            })
        .when(uiSession)
        .exec(any(Runnable.class));

    ramRoot = "ram:///" + UUID.randomUUID();
    FileObject root = HopVfs.getFileObject(ramRoot);
    root.createFolder();
    write(ramRoot + "/docs/index.html", HTML);
    write(ramRoot + "/docs/assets/css/site.css", CSS);
    write(ramRoot + "/docs/100%25 done.html", HTML);
    write(ramRoot + "/report:2026-09-11.html", HTML);
    write(ramRoot + "/secret.hpl", "<pipeline/>".getBytes(StandardCharsets.UTF_8));

    lease = ExplorerFileRegistry.getOrCreate(uiSession, root.getName().getURI());
  }

  @AfterEach
  void tearDown() throws Exception {
    ExplorerFileRegistry.clear();
    FileObject root = HopVfs.getFileObject(ramRoot);
    if (root.exists()) {
      root.deleteAll();
    }
  }

  @Test
  void servesHtmlUnderRoot() throws Exception {
    TestOutputStream output = new TestOutputStream();
    HttpServletResponse response = response(output);

    servlet.doGet(request("/" + lease.getToken() + "/docs/index.html"), response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    verify(response).setContentType("text/html; charset=UTF-8");
    verify(response).setHeader("X-Content-Type-Options", "nosniff");
    verify(response).setHeader("Cache-Control", "private, no-store");
    verify(response)
        .setHeader("Content-Security-Policy", ExplorerFileServlet.CONTENT_SECURITY_POLICY);
    assertArrayEquals(HTML, output.bytes.toByteArray());
  }

  @Test
  void sessionRotationAllowsAccess() throws Exception {
    when(httpSession.getId()).thenReturn("rotated-session");
    TestOutputStream output = new TestOutputStream();
    HttpServletResponse response = response(output);

    servlet.doGet(request("/" + lease.getToken() + "/docs/index.html"), response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    assertArrayEquals(HTML, output.bytes.toByteArray());
  }

  @Test
  void servesFilesWithPercentAndColon() throws Exception {
    TestOutputStream output1 = new TestOutputStream();
    HttpServletResponse response1 = response(output1);
    servlet.doGet(request("/" + lease.getToken() + "/docs/100% done.html"), response1);
    verify(response1).setStatus(HttpServletResponse.SC_OK);
    assertArrayEquals(HTML, output1.bytes.toByteArray());

    TestOutputStream output2 = new TestOutputStream();
    HttpServletResponse response2 = response(output2);
    servlet.doGet(request("/" + lease.getToken() + "/report:2026-09-11.html"), response2);
    verify(response2).setStatus(HttpServletResponse.SC_OK);
    assertArrayEquals(HTML, output2.bytes.toByteArray());
  }

  @Test
  void servesCssNextToHtml() throws Exception {
    TestOutputStream output = new TestOutputStream();
    HttpServletResponse response = response(output);

    servlet.doGet(request("/" + lease.getToken() + "/docs/assets/css/site.css"), response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    verify(response).setContentType("text/css; charset=UTF-8");
    assertArrayEquals(CSS, output.bytes.toByteArray());
  }

  @Test
  void unknownTokenIsNotFound() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    servlet.doGet(request("/" + UUID.randomUUID() + "/docs/index.html"), response);
    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
    verify(response, never()).getOutputStream();
  }

  @Test
  void sessionMismatchIsNotFound() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    HttpServletRequest request = request("/" + lease.getToken() + "/docs/index.html");
    HttpSession other = mock(HttpSession.class);
    when(other.getId()).thenReturn("other-session");
    when(request.getSession(false)).thenReturn(other);

    servlet.doGet(request, response);

    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
  }

  @Test
  void pathEscapeIsNotFound() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    servlet.doGet(request("/" + lease.getToken() + "/docs/../../secret.hpl"), response);
    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
  }

  @Test
  void unknownExtensionIsNotFound() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    servlet.doGet(request("/" + lease.getToken() + "/secret.hpl"), response);
    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
  }

  @Test
  void publicUrlIsRelative() {
    String url = ExplorerFileServing.buildPublicPath("", lease.getToken(), "docs/index.html");
    assertTrue(url.startsWith("/explorer-file/"));
    assertFalse(url.startsWith("http"));
    assertEquals("/explorer-file/" + lease.getToken() + "/docs/index.html", url);
  }

  private HttpServletRequest request(String pathInfo) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getPathInfo()).thenReturn(pathInfo);
    when(request.getSession(false)).thenReturn(httpSession);
    return request;
  }

  private static HttpServletResponse response(TestOutputStream output) throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream()).thenReturn(output);
    return response;
  }

  private static void write(String path, byte[] content) throws Exception {
    FileObject file = HopVfs.getFileObject(path);
    file.getParent().createFolder();
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write(content);
    }
  }

  private static final class TestOutputStream extends ServletOutputStream {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    @Override
    public void write(int value) {
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
