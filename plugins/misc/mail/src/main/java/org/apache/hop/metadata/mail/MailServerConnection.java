package org.apache.hop.metadata.mail;

import lombok.Getter;
import lombok.Setter;
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

  @HopMetadataProperty private String connectionProtocol;

  public MailServerConnection() {}

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

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  @Override
  public void setName(String name) {
    this.name = name;
  }
}
