package org.apache.hop.metadata.mail;

import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.Transport;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
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

  @HopMetadataProperty private String protocol;

  @HopMetadataProperty private String serverHost;

  @HopMetadataProperty private String serverPort;

  @HopMetadataProperty private boolean useAuthentication;

  @HopMetadataProperty private String username;

  @HopMetadataProperty private String password;

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
  }

  public MailServerConnection(IVariables variables) {
    this();
    this.variables = variables;
  }

  public Session getSession(IVariables variables) {
    this.variables = variables;

    if (protocol.equals("SMTP")) {
      // Send an e-mail...
      // create some properties and get the default Session
      Properties props = new Properties();
      //    if (Utils.isEmpty(serverHost)) {
      //      logError(BaseMessages.getString(PKG, "ActionMail.Error.HostNotSpecified"));
      //    }

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

      session = Session.getInstance(props);
    }

    return session;
  }

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
