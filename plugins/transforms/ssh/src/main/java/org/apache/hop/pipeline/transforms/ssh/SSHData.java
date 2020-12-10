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

import com.google.common.annotations.VisibleForTesting;
import com.trilead.ssh2.Connection;
import com.trilead.ssh2.HTTPProxyData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
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

/**
 * @author Samatar
 * @since 03-Juin-2008
 */
public class SSHData extends BaseTransformData implements ITransformData {
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

  public SSHData() {
    super();
    this.indexOfCommand = -1;
    this.conn = null;
    this.wroteOneRow = false;
    this.commands = null;
    this.stdOutField = null;
    this.stdTypeField = null;
  }

  public static Connection OpenConnection( String serveur, int port, String username, String password,
                                           boolean useKey, String keyFilename, String passPhrase, int timeOut, IVariables variables, String proxyhost,
                                           int proxyport, String proxyusername, String proxypassword ) throws HopException {
    Connection conn = null;
    char[] content = null;
    boolean isAuthenticated = false;
    try {
      // perform some checks
      if ( useKey ) {
        if ( Utils.isEmpty( keyFilename ) ) {
          throw new HopException( BaseMessages.getString( SSHMeta.PKG, "SSH.Error.PrivateKeyFileMissing" ) );
        }
        FileObject keyFileObject = HopVfs.getFileObject( keyFilename );

        if ( !keyFileObject.exists() ) {
          throw new HopException( BaseMessages.getString( SSHMeta.PKG, "SSH.Error.PrivateKeyNotExist", keyFilename ) );
        }

        FileContent keyFileContent = keyFileObject.getContent();

        CharArrayWriter charArrayWriter = new CharArrayWriter( (int) keyFileContent.getSize() );

        try ( InputStream in = keyFileContent.getInputStream() ) {
          IOUtils.copy( in, charArrayWriter );
        }

        content = charArrayWriter.toCharArray();
      }
      // Create a new connection
      conn = createConnection( serveur, port );

      /* We want to connect through a HTTP proxy */
      if ( !Utils.isEmpty( proxyhost ) ) {
        /* Now connect */
        // if the proxy requires basic authentication:
        if ( !Utils.isEmpty( proxyusername ) ) {
          conn.setProxyData( new HTTPProxyData( proxyhost, proxyport, proxyusername, proxypassword ) );
        } else {
          conn.setProxyData( new HTTPProxyData( proxyhost, proxyport ) );
        }
      }

      // and connect
      if ( timeOut == 0 ) {
        conn.connect();
      } else {
        conn.connect( null, 0, timeOut * 1000 );
      }
      // authenticate
      if ( useKey ) {
        isAuthenticated =
          conn.authenticateWithPublicKey( username, content, variables.resolve( passPhrase ) );
      } else {
        isAuthenticated = conn.authenticateWithPassword( username, password );
      }
      if ( isAuthenticated == false ) {
        throw new HopException( BaseMessages.getString( SSHMeta.PKG, "SSH.Error.AuthenticationFailed", username ) );
      }
    } catch ( Exception e ) {
      // Something wrong happened
      // do not forget to disconnect if connected
      if ( conn != null ) {
        conn.close();
      }
      throw new HopException( BaseMessages.getString( SSHMeta.PKG, "SSH.Error.ErrorConnecting", serveur, username ), e );
    }
    return conn;
  }

  @VisibleForTesting
  static Connection createConnection( String serveur, int port ) {
    return new Connection( serveur, port );
  }
}
