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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
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

@SuppressWarnings("java:S1104")
public class SshData extends BaseTransformData implements ITransformData {
  private static final Class<?> PKG = SshMeta.class;

  public int indexOfCommand;
  public Session session;
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
    this.session = null;
    this.wroteOneRow = false;
    this.commands = null;
    this.stdOutField = null;
    this.stdTypeField = null;
  }

  public static Session openConnection(IVariables variables, SshMeta meta) throws HopException {
    Session session = null;

    String hostname = variables.resolve(meta.getServerName());
    int port = Const.toInt(variables.resolve(meta.getPort()), 22);
    String username = variables.resolve(meta.getUserName());
    String password =
        Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getPassword()));
    String passPhrase =
        Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getPassPhrase()));

    try {
      JSch jsch = new JSch();

      if (meta.isUsePrivateKey()) {
        String keyFilename = variables.resolve(meta.getKeyFileName());
        if (StringUtils.isEmpty(keyFilename)) {
          throw new HopException(BaseMessages.getString(PKG, "SSH.Error.PrivateKeyFileMissing"));
        }
        FileObject keyFileObject = HopVfs.getFileObject(keyFilename, variables);
        if (!keyFileObject.exists()) {
          throw new HopException(
              BaseMessages.getString(PKG, "SSH.Error.PrivateKeyNotExist", keyFilename));
        }
        if (keyFileObject.getName().getURI().startsWith("file:")) {
          String localKeyPath = HopVfs.getFilename(keyFileObject);
          if (Utils.isEmpty(passPhrase)) {
            jsch.addIdentity(localKeyPath);
          } else {
            jsch.addIdentity(localKeyPath, passPhrase);
          }
        } else {
          byte[] keyBytes = keyFileObject.getContent().getByteArray();
          byte[] passphraseBytes =
              Utils.isEmpty(passPhrase) ? new byte[0] : passPhrase.getBytes(StandardCharsets.UTF_8);
          jsch.addIdentity(username, keyBytes, null, passphraseBytes);
        }
      }

      session = jsch.getSession(username, hostname, port);

      if (!meta.isUsePrivateKey() && !Utils.isEmpty(password)) {
        session.setPassword(password);
      }

      Properties config = new Properties();
      config.put("StrictHostKeyChecking", "no");
      config.put("PreferredAuthentications", "publickey,keyboard-interactive,password");
      session.setConfig(config);

      String proxyHost = variables.resolve(meta.getProxyHost());
      int proxyPort = Const.toInt(variables.resolve(meta.getProxyPort()), 80);
      String proxyUsername = variables.resolve(meta.getProxyUsername());
      String proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getProxyPassword()));

      if (!Utils.isEmpty(proxyHost)) {
        ProxyHTTP proxy = new ProxyHTTP(proxyHost + ":" + proxyPort);
        if (!Utils.isEmpty(proxyUsername)) {
          proxy.setUserPasswd(proxyUsername, proxyPassword);
        }
        session.setProxy(proxy);
      }

      int timeOut = Const.toInt(variables.resolve(meta.getTimeOut()), 0);
      if (timeOut == 0) {
        session.connect();
      } else {
        session.connect(timeOut * 1000);
      }

      if (!session.isConnected()) {
        throw new HopException(
            BaseMessages.getString(PKG, "SSH.Error.AuthenticationFailed", username));
      }
    } catch (JSchException e) {
      if (session != null) {
        session.disconnect();
      }
      if (isAuthFailure(e)) {
        throw new HopException(
            BaseMessages.getString(PKG, "SSH.Error.AuthenticationFailed", username), e);
      }
      throw new HopException(
          BaseMessages.getString(PKG, "SSH.Error.ErrorConnecting", hostname, username), e);
    } catch (Exception e) {
      if (session != null) {
        session.disconnect();
      }
      throw new HopException(
          BaseMessages.getString(PKG, "SSH.Error.ErrorConnecting", hostname, username), e);
    }
    return session;
  }

  private static boolean isAuthFailure(JSchException e) {
    String message = e.getMessage();
    return message != null && message.toLowerCase().contains("auth fail");
  }
}
