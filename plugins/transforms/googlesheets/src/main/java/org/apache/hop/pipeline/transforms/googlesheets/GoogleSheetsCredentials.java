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
 *
 */
package org.apache.hop.pipeline.transforms.googlesheets;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.vfs.HopVfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/** Describe your transform plugin. */
public class GoogleSheetsCredentials {

  public static final String APPLICATION_NAME = "Apache-Hop-Google-Sheets";

  public static Credential getCredentialsJson( String scope, String jsonCredentialPath)
      throws IOException {

    Credential credential;

    InputStream in;
    try {
      in = HopVfs.getInputStream(jsonCredentialPath);
    } catch (HopFileException ex) {
      throw new IOException("Error opening JSON Credentials file " + jsonCredentialPath, ex);
    }

    if (in == null) {
      throw new FileNotFoundException("Resource not found:" + jsonCredentialPath);
    }
    credential = GoogleCredential.fromStream(in).createScoped(Collections.singleton(scope));
    return credential;
  }
}
