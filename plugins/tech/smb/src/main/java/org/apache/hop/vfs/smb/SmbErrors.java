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
package org.apache.hop.vfs.smb;

import com.hierynomus.mserref.NtStatus;
import com.hierynomus.mssmb2.SMBApiException;
import java.io.IOException;

/** Turns smbj status codes into the two outcomes VFS cares about: missing, or a real failure. */
final class SmbErrors {
  private SmbErrors() {}

  static boolean notFound(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof SmbNotFoundException) {
        return true;
      }
      if (current instanceof SMBApiException api) {
        NtStatus status = api.getStatus();
        return status == NtStatus.STATUS_OBJECT_NAME_NOT_FOUND
            || status == NtStatus.STATUS_OBJECT_PATH_NOT_FOUND
            || status == NtStatus.STATUS_NO_SUCH_FILE
            || status == NtStatus.STATUS_NOT_FOUND;
      }
      current = current.getCause();
    }
    return false;
  }

  static IOException io(Throwable error) {
    if (error instanceof IOException ioException) {
      return ioException;
    }
    String message = error.getMessage();
    if (message == null || message.isBlank()) {
      message = error.getClass().getSimpleName();
    }
    return new IOException(message, error);
  }

  /** The path is not on the share. Distinct from access denied. */
  static final class SmbNotFoundException extends IOException {
    SmbNotFoundException(String sharePath) {
      super(sharePath == null || sharePath.isEmpty() ? "SMB path not found" : sharePath);
    }
  }
}
