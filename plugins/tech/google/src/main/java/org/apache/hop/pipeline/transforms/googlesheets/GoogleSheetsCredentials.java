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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.sqladmin.SQLAdminScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/** Describe your transform plugin. */
public class GoogleSheetsCredentials {

  public static final String APPLICATION_NAME = "Apache-Hop-Google-Sheets";

  public static HttpCredentialsAdapter getCredentialsJson(
      String scope, String jsonCredentialPath, String impersonation, IVariables variables)
      throws IOException {

    GoogleCredentials credential;

    InputStream in;
    try {
      in = HopVfs.getInputStream(jsonCredentialPath, variables);
    } catch (HopFileException ex) {
      throw new IOException("Error opening JSON Credentials file " + jsonCredentialPath, ex);
    }

    if (in == null) {
      throw new FileNotFoundException("Resource not found:" + jsonCredentialPath);
    }
    if (StringUtils.isEmpty(impersonation)) {
      credential = GoogleCredentials.fromStream(in).createScoped(Collections.singleton(scope));
    } else {
      credential =
          GoogleCredentials.fromStream(in)
              .createScoped(Collections.singleton(SQLAdminScopes.SQLSERVICE_ADMIN))
              .createDelegated(impersonation);
    }

    return new HttpCredentialsAdapter(credential);
  }

  public static HttpRequestInitializer setHttpTimeout(
      final HttpRequestInitializer requestInitializer, final String timeout) {
    return httpRequest -> {
      Integer TO = Integer.parseInt(timeout);
      requestInitializer.initialize(httpRequest);
      httpRequest.setConnectTimeout(TO * 60000); // 10 minutes connect timeout
      httpRequest.setReadTimeout(TO * 60000); // 10 minutes read timeout
    };
  }
}
