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
package org.apache.hop.pipeline.transforms.googlesheet;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import org.apache.hop.core.vfs.HopVfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/** Describe your transform plugin. */
public class GoogleSheetsPluginCredentials {

  public static Credential getCredentialsJson(String scope, String jsonCredentialPath)
      throws IOException {

    Credential credential = null;
    // InputStream in =
    // GoogleSheetsPluginCredentials.class.getResourceAsStream("/plugins/transforms/googlesheet/credentials/client_secret.json");//pentaho-sheets-261911-18ce0057e3d3.json
    // logBasic("Getting credential json file from :"+Const.getKettleDirectory());
    InputStream in = null;
    try {
      in =
          HopVfs.getInputStream(
              jsonCredentialPath); // Const.getKettleDirectory() + "/client_secret.json");
    } catch (Exception e) {
      // throw new HopFileException("Exception",e.getMessage(),e);
    }

    if (in == null) {
      throw new FileNotFoundException("Resource not found:" + jsonCredentialPath);
    }
    credential = GoogleCredential.fromStream(in).createScoped(Collections.singleton(scope));
    return credential;
  }
}
