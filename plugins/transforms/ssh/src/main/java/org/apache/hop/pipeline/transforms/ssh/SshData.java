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

package org.apache.hop.pipeline.transforms.ssh;

import com.trilead.ssh2.Connection;
import com.trilead.ssh2.HTTPProxyData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.CharArrayWriter;
import java.io.InputStream;

public class SshData extends BaseTransformData implements ITransformData {
  public int indexOfCommand;
  public Connection conn;
  public boolean wroteOneRow;
  public String commands;
  public int nrInputFields;
  public int nrOutputFields;

  // Output fields
  public String stdOutField;
  public String stdTypeField;

  public IRowMeta outputRowMeta;

  public SshData() {
    super();
    this.indexOfCommand = -1;
    this.conn = null;
    this.wroteOneRow = false;
    this.commands = null;
    this.stdOutField = null;
    this.stdTypeField = null;
  }

  public static Connection openConnection(IVariables variables, SshMeta meta) throws HopException {
    Connection connection = null;
    char[] content = null;
    boolean isAuthenticated;

    String hostname = variables.resolve(meta.getServerName());
    int port = Const.toInt(variables.resolve(meta.getPort()), 22);
    String username = variables.resolve(meta.getUserName());
    String password =
        Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getPassword()));
    String passPhrase =
        Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getPassPhrase()));

    try {
      // perform some checks
      if (meta.isUsePrivateKey()) {
        String keyFilename = variables.resolve(meta.getKeyFileName());
        if (StringUtils.isEmpty(keyFilename)) {
          throw new HopException(
              BaseMessages.getString(SshMeta.PKG, "SSH.Error.PrivateKeyFileMissing"));
        }
        FileObject keyFileObject = HopVfs.getFileObject(keyFilename);

        if (!keyFileObject.exists()) {
          throw new HopException(
              BaseMessages.getString(SshMeta.PKG, "SSH.Error.PrivateKeyNotExist", keyFilename));
        }

        FileContent keyFileContent = keyFileObject.getContent();

        CharArrayWriter charArrayWriter = new CharArrayWriter((int) keyFileContent.getSize());

        try (InputStream in = keyFileContent.getInputStream()) {
          IOUtils.copy(in, charArrayWriter, "UTF-8");
        }

        content = charArrayWriter.toCharArray();
      }

      // Create a new connection
      connection = new Connection(hostname, port);

      String proxyHost = variables.resolve(meta.getProxyHost());
      int proxyPort = Const.toInt(variables.resolve(meta.getProxyPort()), 23);
      String proxyUsername = variables.resolve(meta.getProxyUsername());
      String proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getProxyPassword()));

      /* We want to connect through a HTTP proxy */
      if (!Utils.isEmpty(proxyHost)) {
        /* Now connect */
        // if the proxy requires basic authentication:
        if (!Utils.isEmpty(proxyUsername)) {
          connection.setProxyData(
              new HTTPProxyData(proxyHost, proxyPort, proxyUsername, proxyPassword));
        } else {
          connection.setProxyData(new HTTPProxyData(proxyHost, proxyPort));
        }
      }

      int timeOut = Const.toInt(variables.resolve(meta.getTimeOut()), 0);

      // and connect
      if (timeOut == 0) {
        connection.connect();
      } else {
        connection.connect(null, 0, timeOut * 1000);
      }

      // authenticate
      if (meta.isUsePrivateKey()) {
        isAuthenticated =
            connection.authenticateWithPublicKey(username, content, variables.resolve(passPhrase));
      } else {
        isAuthenticated = connection.authenticateWithPassword(username, password);
      }
      if (!isAuthenticated) {
        throw new HopException(
            BaseMessages.getString(SshMeta.PKG, "SSH.Error.AuthenticationFailed", username));
      }
    } catch (Exception e) {
      // Something wrong happened
      // do not forget to disconnect if connected
      if (connection != null) {
        connection.close();
      }
      throw new HopException(
          BaseMessages.getString(SshMeta.PKG, "SSH.Error.ErrorConnecting", hostname, username), e);
    }
    return connection;
  }
}
