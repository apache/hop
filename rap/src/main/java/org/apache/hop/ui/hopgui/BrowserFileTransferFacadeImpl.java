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

package org.apache.hop.ui.hopgui;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;

/** Browser uploads and downloads for Hop Web, with one transfer per user session. */
public class BrowserFileTransferFacadeImpl extends BrowserFileTransferFacade {

  private static final String SESSION_TRANSFER =
      BrowserFileTransferFacadeImpl.class.getName() + ".transfer";

  @Override
  boolean isSupportedInternal() {
    return true;
  }

  @Override
  void uploadInternal(long sizeLimit, IUploadListener listener) {
    UserFileTransfer transfer;
    try {
      transfer = transfer();
    } catch (Exception e) {
      listener.error(e);
      return;
    }
    transfer.open(
        null,
        sizeLimit,
        new UserFileTransfer.UploadListener() {
          @Override
          public void uploaded(String filename, Path uploadedFile) throws Exception {
            // The transfer deletes its upload folder when this returns. Hand the listener a copy
            // it owns, so it can store the file in the background.
            Path owned =
                Files.createTempFile(
                    HopWebUserFilePlugin.getSessionTempDirectory(), "upload-", ".tmp");
            Files.move(uploadedFile, owned, StandardCopyOption.REPLACE_EXISTING);
            listener.uploaded(filename, owned);
          }

          @Override
          public void error(Exception exception) {
            listener.error(exception);
          }
        });
  }

  @Override
  void downloadInternal(String filename, Path source) throws IOException {
    transfer().download(filename, "application/octet-stream", source);
  }

  private static UserFileTransfer transfer() throws IOException {
    UISession session = RWT.getUISession();
    Object existing = session.getAttribute(SESSION_TRANSFER);
    if (existing instanceof UserFileTransfer transfer && !transfer.isDisposed()) {
      return transfer;
    }
    UserFileTransfer transfer =
        new UserFileTransfer(
            HopGui.getInstance().getShell(), HopWebUserFilePlugin.getSessionTempDirectory());
    session.setAttribute(SESSION_TRANSFER, transfer);
    return transfer;
  }
}
