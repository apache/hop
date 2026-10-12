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
import java.nio.file.Path;

/**
 * Moves a file between the user's browser and the server, for Hop Web. On the desktop there is no
 * browser, and {@link #isSupported()} is false.
 */
public abstract class BrowserFileTransferFacade {

  /** Receives a file the user uploaded from the browser. */
  public interface IUploadListener {
    /**
     * @param filename the name of the file on the user's computer, without a folder
     * @param uploadedFile a server-side temporary copy. The listener owns it and deletes it.
     */
    void uploaded(String filename, Path uploadedFile) throws Exception;

    void error(Exception exception);
  }

  private static final BrowserFileTransferFacade IMPL = load();

  private static BrowserFileTransferFacade load() {
    try {
      return (BrowserFileTransferFacade)
          ImplementationLoader.newInstance(BrowserFileTransferFacade.class);
    } catch (Throwable e) {
      // The desktop and the hop-ui unit tests have no browser to transfer to.
      return new BrowserFileTransferFacade() {
        @Override
        boolean isSupportedInternal() {
          return false;
        }

        @Override
        void uploadInternal(long sizeLimit, IUploadListener listener) {
          listener.error(new IOException("There is no browser to upload from."));
        }

        @Override
        void downloadInternal(String filename, Path source) throws IOException {
          throw new IOException("There is no browser to download to.");
        }
      };
    }
  }

  /** True on Hop Web. */
  public static boolean isSupported() {
    return IMPL.isSupportedInternal();
  }

  /**
   * Let the user choose a file in the browser and upload it. Call on the UI thread. Returns once
   * the upload finished or was cancelled.
   *
   * @param sizeLimit the largest file accepted, in bytes
   * @param listener receives the uploaded file or the error
   */
  public static void upload(long sizeLimit, IUploadListener listener) {
    IMPL.uploadInternal(sizeLimit, listener);
  }

  /**
   * Offer {@code source} to the browser as a download named {@code filename}. Call on the UI
   * thread. The content is copied, so the caller may delete {@code source} afterwards.
   */
  public static void download(String filename, Path source) throws IOException {
    IMPL.downloadInternal(filename, source);
  }

  abstract boolean isSupportedInternal();

  abstract void uploadInternal(long sizeLimit, IUploadListener listener);

  abstract void downloadInternal(String filename, Path source) throws IOException;
}
