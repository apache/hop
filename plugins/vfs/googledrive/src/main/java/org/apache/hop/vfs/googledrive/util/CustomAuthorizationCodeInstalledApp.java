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

package org.apache.hop.vfs.googledrive.util;

import com.google.api.client.auth.oauth2.AuthorizationCodeFlow;
import com.google.api.client.auth.oauth2.AuthorizationCodeRequestUrl;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.VerificationCodeReceiver;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.vfs.googledrive.ui.GoogleAuthorizationDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import java.io.IOException;

public class CustomAuthorizationCodeInstalledApp extends AuthorizationCodeInstalledApp {

  public CustomAuthorizationCodeInstalledApp(
      AuthorizationCodeFlow flow, VerificationCodeReceiver receiver) {
    super(flow, receiver);
  }

  protected void onAuthorization(AuthorizationCodeRequestUrl authorizationUrl) throws IOException {
    String url = authorizationUrl.build();
    HopGui hopGui = HopGui.getInstance();
    if (hopGui != null) {

      Display.getDefault()
          .syncExec(
              () -> {
                Shell shell = hopGui.getShell();
                GoogleAuthorizationDialog authorizationDialog =
                    new GoogleAuthorizationDialog(shell, getReceiver());
                authorizationDialog.open(url);
              });

    } else {
      browse(url);
    }
  }
}
