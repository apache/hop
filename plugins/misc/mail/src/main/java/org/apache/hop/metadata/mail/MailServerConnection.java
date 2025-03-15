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

package org.apache.hop.metadata.mail;

import com.sun.mail.imap.IMAPSSLStore;
import com.sun.mail.pop3.POP3SSLStore;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.Transport;
import jakarta.mail.URLName;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@Getter
@Setter
@HopMetadata(
    key = "MailServerConnection",
    name = "i18n::MailServerConnection.name",
    description = "i18n::MailServerConnection.description",
    image = "mail.svg",
    documentationUrl = "",
    hopMetadataPropertyType = HopMetadataPropertyType.MAIL_SERVER_CONNECTION)
public class MailServerConnection extends HopMetadataBase implements IHopMetadata {

  private static final Class<?> PKG = MailServerConnection.class;
  public static final String CONST_MAIL = "mail.";

  private Session session;
  private IVariables variables;
  private Properties props;
  private Store store;

  @HopMetadataProperty private String protocol;

  @HopMetadataProperty private String serverHost;

  @HopMetadataProperty private String serverPort;

  @HopMetadataProperty private boolean useAuthentication;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private boolean useXOAuth2;

  @HopMetadataProperty private boolean useSecureAuthentication;

  @HopMetadataProperty private String secureConnectionType;

  @HopMetadataProperty private boolean useProxy;

  @HopMetadataProperty private String proxyHost;

  @HopMetadataProperty private String proxyUsername;

  @HopMetadataProperty private String trustedHosts;

  @HopMetadataProperty private boolean checkServerIdentity;

  public MailServerConnection() {
    super();
    props = new Properties();
  }

  public MailServerConnection(IVariables variables) {
    this();
    this.variables = variables;
  }

  public Session getSession(IVariables variables, ILogChannel log) {
    this.variables = variables;

    // SMTP
    if (protocol.equals("SMTP")) {
      // Send an e-mail...
      // create some properties and get the default Session
      if (Utils.isEmpty(serverHost)) {
        log.logError(BaseMessages.getString(PKG, "ActionMail.Error.HostNotSpecified"));
      }

      protocol = "smtp";
      if (useSecureAuthentication) {
        if (useXOAuth2) {
          props.put("mail.smtp.auth.mechanisms", "XOAUTH2");
        }
        if (secureConnectionType.equals("TLS")) {
          // Allow TLS authentication
          props.put("mail.smtp.starttls.enable", "true");
        } else if (secureConnectionType.equals("TLS 1.2")) {
          // Allow TLS 1.2 authentication
          props.put("mail.smtp.starttls.enable", "true");
          props.put("mail.smtp.ssl.protocols", "TLSv1.2");
        } else {

          protocol = "smtps";
          // required to get rid of a SSL exception :
          // nested exception is:
          // javax.net.ssl.SSLException: Unsupported record version Unknown
          props.put("mail.smtps.quitwait", "false");
        }
        props.put("mail.smtp.ssl.checkServerIdentity", isCheckServerIdentity());
        if (!Utils.isEmpty(trustedHosts)) {
          props.put("mail.smtp.ssl.trust", variables.resolve(trustedHosts));
        }
      }

      props.put(CONST_MAIL + protocol.toLowerCase() + ".host", variables.resolve(serverHost));
      if (!Utils.isEmpty(serverPort)) {
        props.put(CONST_MAIL + protocol.toLowerCase() + ".port", variables.resolve(serverPort));
      }

      //    if (isDebug()) {
      //      props.put("mail.debug", "true");
      //    }

      if (useAuthentication) {
        props.put(CONST_MAIL + protocol + ".auth", "true");
      }
    } else {
      String protocolString = "";
      if (protocol.equals("POP3")) {
        props.setProperty("mail.pop3s.rsetbeforequit", "true");
        props.setProperty("mail.pop3.rsetbeforequit", "true");
        protocolString = "pop3";
      } else if (protocol.equals("MBOX")) {
        props.setProperty(
            "mstor.mbox.metadataStrategy", "none"); // mstor.mbox.metadataStrategy={none|xml|yaml}
        props.setProperty("mstor.cache.disabled", "true"); // prevent diskstore fail
        protocolString = "mstor";
      } else if (protocol.equals("IMAP")) {
        protocolString = "imap";
      }

      if (useSecureAuthentication && !protocol.equals("MBOX")) {
        // Supports IMAP/POP3 connection with SSL, the connection is established via SSL.
        props.setProperty(
            CONST_MAIL + protocolString + ".socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.setProperty(CONST_MAIL + protocolString + ".socketFactory.fallback", "false");
        props.setProperty(
            CONST_MAIL + protocolString + ".port", "" + variables.resolve(serverPort));
        props.setProperty(
            CONST_MAIL + protocolString + ".socketFactory.port",
            "" + variables.resolve(serverPort));
        if (useXOAuth2) {
          props.setProperty(CONST_MAIL + protocolString + ".ssl.enable", "true");
          props.setProperty(CONST_MAIL + protocolString + ".auth.mechanisms", "XOAUTH2");
        }
      } else {

      }
    }
    session = Session.getInstance(props);
    session.setDebug(log.isDebug());

    return session;
  }

  // SMTP
  public Transport getTransport() throws MessagingException {
    Transport transport = session.getTransport(protocol);
    String authPass = getPassword(password);

    if (useAuthentication) {
      if (!Utils.isEmpty(serverPort)) {
        transport.connect(
            variables.resolve(Const.NVL(serverHost, "")),
            Integer.parseInt(variables.resolve(Const.NVL(serverPort, ""))),
            variables.resolve(Const.NVL(username, "")),
            authPass);
      } else {
        transport.connect(
            variables.resolve(Const.NVL(serverHost, "")),
            variables.resolve(Const.NVL(username, "")),
            authPass);
      }
    } else {
      transport.connect();
    }

    return transport;
  }

  // IMAP, POP, MBOX
  private Store getStore() throws MessagingException {
    if (useSecureAuthentication && !protocol.equals("MBOX")) {
      URLName url =
          new URLName(
              protocol,
              variables.resolve(serverHost),
              Integer.valueOf(variables.resolve(serverPort)),
              "",
              variables.resolve(username),
              variables.resolve(password));

      switch (protocol) {
        case "POP3":
          store = new POP3SSLStore(session, url);
          break;
        case "IMAP":
          store = new IMAPSSLStore(session, url);
        default:
          break;
      }
    } else {
      if (protocol.equals("MBOX")) {
        this.store = this.session.getStore(new URLName(protocol + ":" + serverHost));
      } else {
        this.store = this.session.getStore(protocol);
      }
    }

    return store;
  }

  public MailServerConnection(MailServerConnection connection) {}

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }

  @Override
  public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override
  public boolean equals(Object object) {

    if (object == this) {
      return true;
    }
    if (!(object instanceof MailServerConnection)) {
      return false;
    }

    MailServerConnection connection = (MailServerConnection) object;

    return name != null && name.equalsIgnoreCase(connection.name);
  }

  public void testConnection(Session session) throws MessagingException {
    Store store = session.getStore();
    store.connect();
  }

  public String getPassword(String authPassword) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(Const.NVL(authPassword, "")));
  }
}
