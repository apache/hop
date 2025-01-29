package org.apache.hop.metadata.mail;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.CheckBoxVar;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class MailServerConnectionEditor extends MetadataEditor<MailServerConnection> {
  private static final Class<?> PKG = MailServerConnectionEditor.class;

  private Text wName;

  private TextVar wServerHost;

  private TextVar wServerPort;

  private CheckBoxVar wUseAuthentication;

  private CheckBoxVar wUseXOAuth2;

  private TextVar wServerUsername;

  private PasswordTextVar wServerPassword;

  private CheckBoxVar wUseSecureAuthentication;

  private ComboVar wSecureConnectionType;

  private CheckBoxVar wUseProxy;

  private TextVar wProxyUsername;

  private PasswordTextVar wProxyPassword;

  private ComboVar wConnectionProtocol;

  public MailServerConnectionEditor(
      HopGui hopGui, MetadataManager<MailServerConnection> manager, MailServerConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite composite) {
    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    IVariables variables = hopGui.getVariables();

    // Name
    Label wlName = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "MailServerConnectionDialog.Name"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0); // To the right of the label
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlServerHostLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlServerHostLabel);
    wlServerHostLabel.setText(BaseMessages.getString(PKG, "MailServerConnectionDialog.ServerHost"));
    FormData fdlServerHostLabel = new FormData();
    fdlServerHostLabel.top = new FormAttachment(lastControl, margin);
    fdlServerHostLabel.left = new FormAttachment(0, 0);
    fdlServerHostLabel.right = new FormAttachment(middle, -margin);
    wlServerHostLabel.setLayoutData(fdlServerHostLabel);
    wServerHost = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wServerHost);
    FormData fdServerHost = new FormData();
    fdServerHost.top = new FormAttachment(lastControl, margin);
    fdServerHost.left = new FormAttachment(middle, 0);
    fdServerHost.right = new FormAttachment(100, 0);
    wServerHost.setLayoutData(fdServerHost);
    lastControl = wServerHost;

    Label wlServerPortLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlServerPortLabel);
    wlServerPortLabel.setText(BaseMessages.getString(PKG, "MailServerConnectionDialog.ServerPort"));
    FormData fdlServerPortLabel = new FormData();
    fdlServerPortLabel.top = new FormAttachment(lastControl, margin);
    fdlServerPortLabel.left = new FormAttachment(0, 0);
    fdlServerPortLabel.right = new FormAttachment(middle, -margin);
    wlServerPortLabel.setLayoutData(fdlServerPortLabel);
    wServerPort = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wServerPort);
    FormData fdServerPort = new FormData();
    fdServerPort.top = new FormAttachment(lastControl, margin);
    fdServerPort.left = new FormAttachment(middle, 0);
    fdServerPort.right = new FormAttachment(100, 0);
    wServerPort.setLayoutData(fdServerPort);
    lastControl = wServerPort;

    Label wlUseAuthenticationLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlUseAuthenticationLabel);
    wlUseAuthenticationLabel.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.UseAuthentication"));
    FormData fdlUseAuthenticationLabel = new FormData();
    fdlUseAuthenticationLabel.top = new FormAttachment(lastControl, margin);
    fdlUseAuthenticationLabel.left = new FormAttachment(0, 0);
    fdlUseAuthenticationLabel.right = new FormAttachment(middle, -margin);
    wlUseAuthenticationLabel.setLayoutData(fdlUseAuthenticationLabel);
    wUseAuthentication = new CheckBoxVar(variables, composite, SWT.CHECK);
    PropsUi.setLook(wUseAuthentication);
    FormData fdUseAuthentication = new FormData();
    fdUseAuthentication.top = new FormAttachment(lastControl, margin);
    fdUseAuthentication.left = new FormAttachment(middle, 0);
    fdUseAuthentication.right = new FormAttachment(100, 0);
    wUseAuthentication.setLayoutData(fdUseAuthentication);
    lastControl = wUseAuthentication;

    Label wlUseXOAuth2Label = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlUseXOAuth2Label);
    wlUseXOAuth2Label.setText(BaseMessages.getString(PKG, "MailServerConnectionDialog.UseXOAuth2"));
    FormData fdlUseXOAuth2Label = new FormData();
    fdlUseXOAuth2Label.top = new FormAttachment(lastControl, margin);
    fdlUseXOAuth2Label.left = new FormAttachment(0, 0);
    fdlUseXOAuth2Label.right = new FormAttachment(middle, -margin);
    wlUseXOAuth2Label.setLayoutData(fdlUseXOAuth2Label);
    wUseXOAuth2 = new CheckBoxVar(variables, composite, SWT.CHECK);
    PropsUi.setLook(wUseXOAuth2);
    FormData fdUseXOAuth2 = new FormData();
    fdUseXOAuth2.top = new FormAttachment(lastControl, margin);
    fdUseXOAuth2.left = new FormAttachment(middle, 0);
    fdUseXOAuth2.right = new FormAttachment(100, 0);
    wUseXOAuth2.setLayoutData(fdUseXOAuth2);
    lastControl = wUseXOAuth2;

    Label wlServerUsernameLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlServerUsernameLabel);
    wlServerUsernameLabel.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.Username"));
    FormData fdlServerUsernameLabel = new FormData();
    fdlServerUsernameLabel.top = new FormAttachment(lastControl, margin);
    fdlServerUsernameLabel.left = new FormAttachment(0, 0);
    fdlServerUsernameLabel.right = new FormAttachment(middle, -margin);
    wlServerUsernameLabel.setLayoutData(fdlServerUsernameLabel);
    wServerUsername = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wServerUsername);
    FormData fdServerUsername = new FormData();
    fdServerUsername.top = new FormAttachment(lastControl, margin);
    fdServerUsername.left = new FormAttachment(middle, 0);
    fdServerUsername.right = new FormAttachment(100, 0);
    wServerUsername.setLayoutData(fdServerUsername);
    lastControl = wServerUsername;

    Label wlServerPasswordLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlServerPasswordLabel);
    wlServerPasswordLabel.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.Password"));
    FormData fdlServerPasswordLabel = new FormData();
    fdlServerPasswordLabel.top = new FormAttachment(lastControl, margin);
    fdlServerPasswordLabel.left = new FormAttachment(0, 0);
    fdlServerPasswordLabel.right = new FormAttachment(middle, -margin);
    wlServerPasswordLabel.setLayoutData(fdlServerPasswordLabel);
    wServerPassword = new PasswordTextVar(variables, composite, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wServerPassword);
    FormData fdServerPassword = new FormData();
    fdServerPassword.top = new FormAttachment(lastControl, margin);
    fdServerPassword.left = new FormAttachment(middle, 0);
    fdServerPassword.right = new FormAttachment(100, 0);
    wServerPassword.setLayoutData(fdServerPassword);
    lastControl = wServerPassword;

    Label wlUseSecureAuthenticationLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlUseSecureAuthenticationLabel);
    wlUseSecureAuthenticationLabel.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.UseSecureAuthentication"));
    FormData fdlUseSecureAuthenticationLabel = new FormData();
    fdlUseSecureAuthenticationLabel.top = new FormAttachment(lastControl, margin);
    fdlUseSecureAuthenticationLabel.left = new FormAttachment(0, 0);
    fdlUseSecureAuthenticationLabel.right = new FormAttachment(middle, -margin);
    wlUseSecureAuthenticationLabel.setLayoutData(fdlUseSecureAuthenticationLabel);
    wUseSecureAuthentication = new CheckBoxVar(variables, composite, SWT.CHECK);
    PropsUi.setLook(wUseSecureAuthentication);
    FormData fdUseSecureAuthentication = new FormData();
    fdUseSecureAuthentication.top = new FormAttachment(lastControl, margin);
    fdUseSecureAuthentication.left = new FormAttachment(middle, 0);
    fdUseSecureAuthentication.right = new FormAttachment(100, 0);
    wUseSecureAuthentication.setLayoutData(fdUseSecureAuthentication);
    lastControl = wUseSecureAuthentication;

    Label wlSecureAuthenticationTypeLabel = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlSecureAuthenticationTypeLabel);
    wlSecureAuthenticationTypeLabel.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.UseSecureAuthenticationType"));
    FormData fdlUseSecureAuthenticationTypeLabel = new FormData();
    fdlUseSecureAuthenticationTypeLabel.top = new FormAttachment(lastControl, margin);
    fdlUseSecureAuthenticationTypeLabel.left = new FormAttachment(0, 0);
    fdlUseSecureAuthenticationTypeLabel.right = new FormAttachment(middle, -margin);
    wlSecureAuthenticationTypeLabel.setLayoutData(fdlUseSecureAuthenticationTypeLabel);
    wSecureConnectionType = new ComboVar(variables, composite, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wSecureConnectionType);
    FormData fdSecureConnectionType = new FormData();
    fdSecureConnectionType.top = new FormAttachment(lastControl, margin);
    fdSecureConnectionType.left = new FormAttachment(middle, 0);
    fdSecureConnectionType.right = new FormAttachment(100, 0);
    wSecureConnectionType.setLayoutData(fdSecureConnectionType);
    lastControl = wSecureConnectionType;

    Label wlUseProxy = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlUseProxy);
    wlUseProxy.setText(BaseMessages.getString(PKG, "MailServerConnectionDialog.UseProxy"));
    FormData fdlUseProxy = new FormData();
    fdlUseProxy.top = new FormAttachment(lastControl, margin);
    fdlUseProxy.left = new FormAttachment(0, 0);
    fdlUseProxy.right = new FormAttachment(middle, -margin);
    wlUseProxy.setLayoutData(fdlUseProxy);
    wUseProxy = new CheckBoxVar(variables, composite, SWT.CHECK);
    PropsUi.setLook(wUseProxy);
    FormData fdUseProxy = new FormData();
    fdUseProxy.top = new FormAttachment(lastControl, margin);
    fdUseProxy.left = new FormAttachment(middle, 0);
    fdUseProxy.right = new FormAttachment(100, 0);
    wUseProxy.setLayoutData(fdUseProxy);
    lastControl = wUseProxy;

    Label wlProxyUsername = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlProxyUsername);
    wlProxyUsername.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.ProxyUsername"));
    FormData fdlProxyUsername = new FormData();
    fdlProxyUsername.top = new FormAttachment(lastControl, margin);
    fdlProxyUsername.left = new FormAttachment(0, 0);
    fdlProxyUsername.right = new FormAttachment(middle, -margin);
    wlProxyUsername.setLayoutData(fdlProxyUsername);
    wProxyUsername = new TextVar(variables, composite, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wProxyUsername);
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.top = new FormAttachment(lastControl, margin);
    fdProxyUsername.left = new FormAttachment(middle, 0);
    fdProxyUsername.right = new FormAttachment(100, 0);
    wProxyUsername.setLayoutData(fdProxyUsername);
    lastControl = wProxyUsername;

    Label wlProxyPassword = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlProxyPassword);
    wlProxyPassword.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.ProxyPassword"));
    FormData fdlProxyPassword = new FormData();
    fdlProxyPassword.top = new FormAttachment(lastControl, margin);
    fdlProxyPassword.left = new FormAttachment(0, 0);
    fdlProxyPassword.right = new FormAttachment(middle, -margin);
    wlProxyPassword.setLayoutData(fdlProxyPassword);
    wProxyPassword = new PasswordTextVar(variables, composite, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wProxyPassword);
    FormData fdProxyPassword = new FormData();
    fdProxyPassword.top = new FormAttachment(lastControl, margin);
    fdProxyPassword.left = new FormAttachment(middle, 0);
    fdProxyPassword.right = new FormAttachment(100, 0);
    wProxyPassword.setLayoutData(fdProxyPassword);
    lastControl = wProxyPassword;

    Label wlConnectionProtocol = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlConnectionProtocol);
    wlConnectionProtocol.setText(
        BaseMessages.getString(PKG, "MailServerConnectionDialog.ConnectionProtocol"));
    FormData fdlConnectionProtocol = new FormData();
    fdlConnectionProtocol.top = new FormAttachment(lastControl, margin);
    fdlConnectionProtocol.left = new FormAttachment(0, 0);
    fdlConnectionProtocol.right = new FormAttachment(middle, -margin);
    wlConnectionProtocol.setLayoutData(fdlConnectionProtocol);
    wConnectionProtocol = new ComboVar(variables, composite, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wConnectionProtocol);
    FormData fdConnectionProtocol = new FormData();
    fdConnectionProtocol.top = new FormAttachment(lastControl, margin);
    fdConnectionProtocol.left = new FormAttachment(middle, 0);
    fdConnectionProtocol.right = new FormAttachment(100, 0);
    wConnectionProtocol.setLayoutData(fdConnectionProtocol);
    lastControl = wConnectionProtocol;

    setWidgetsContent();

    Control[] controls = {
      wName,
      wServerHost,
      wServerPort,
      wUseAuthentication,
      wUseXOAuth2,
      wServerUsername,
      wServerPassword,
      wUseSecureAuthentication,
      wSecureConnectionType,
      wUseProxy,
      wProxyUsername,
      wConnectionProtocol
    };
    for (Control control : controls) {
      control.addListener(SWT.Modify, e -> setChanged());
      control.addListener(SWT.Selection, e -> setChanged());
    }
  }

  @Override
  public void setWidgetsContent() {}

  @Override
  public void getWidgetsContent(MailServerConnection meta) {}
}
