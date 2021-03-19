package org.apache.hop.splunk;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog that allows you to edit the settings of a Splunk connection
 *
 * @author Matt
 * @see SplunkConnection
 */
public class SplunkConnectionEditor extends MetadataEditor<SplunkConnection> {
  private static Class<?> PKG =
      SplunkConnectionEditor.class; // for i18n purposes, needed by Translator2!!

  // Connection properties
  //
  private Text wName;
  private TextVar wHostname;
  private TextVar wPort;
  private TextVar wUsername;
  private TextVar wPassword;

  public SplunkConnectionEditor(
      HopGui hopGui, MetadataManager<SplunkConnection> manager, SplunkConnection connection) {
    super(hopGui, manager, connection);
  }

  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN + 2;

    // The name
    Label wlName = new Label(parent, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "SplunkConnectionDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0); // To the right of the label
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    // The Hostname
    Label wlHostname = new Label(parent, SWT.RIGHT);
    props.setLook(wlHostname);
    wlHostname.setText(BaseMessages.getString(PKG, "SplunkConnectionDialog.Hostname.Label"));
    FormData fdlHostname = new FormData();
    fdlHostname.top = new FormAttachment(lastControl, margin);
    fdlHostname.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlHostname.right = new FormAttachment(middle, -margin);
    wlHostname.setLayoutData(fdlHostname);
    wHostname = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wHostname);
    FormData fdHostname = new FormData();
    fdHostname.top = new FormAttachment(wlHostname, 0, SWT.CENTER);
    fdHostname.left = new FormAttachment(middle, 0); // To the right of the label
    fdHostname.right = new FormAttachment(95, 0);
    wHostname.setLayoutData(fdHostname);
    lastControl = wHostname;

    // port?
    Label wlPort = new Label(parent, SWT.RIGHT);
    props.setLook(wlPort);
    wlPort.setText(BaseMessages.getString(PKG, "SplunkConnectionDialog.Port.Label"));
    FormData fdlPort = new FormData();
    fdlPort.top = new FormAttachment(lastControl, margin);
    fdlPort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPort.right = new FormAttachment(middle, -margin);
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPort);
    FormData fdPort = new FormData();
    fdPort.top = new FormAttachment(wlPort, 0, SWT.CENTER);
    fdPort.left = new FormAttachment(middle, 0); // To the right of the label
    fdPort.right = new FormAttachment(95, 0);
    wPort.setLayoutData(fdPort);
    lastControl = wPort;

    // Username
    Label wlUsername = new Label(parent, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "SplunkConnectionDialog.UserName.Label"));
    props.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(lastControl, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, 0);
    wUsername.setLayoutData(fdUsername);
    lastControl = wUsername;

    // Password
    Label wlPassword = new Label(parent, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "SplunkConnectionDialog.Password.Label"));
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(lastControl, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword =
        new PasswordTextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);

    // Set content on these widgets...
    //
    setWidgetsContent();

    // See if anything changed, update the UI accordingly
    //
    wName.addModifyListener(e -> setChanged());
    wUsername.addModifyListener(e -> setChanged());
    wPassword.addModifyListener(e -> setChanged());
    wHostname.addModifyListener(e -> setChanged());
    wPort.addModifyListener(e -> setChanged());
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {

    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> test());

    return new Button[] {
      wTest,
    };
  }

  @Override public void setWidgetsContent() {
    SplunkConnection splunk = getMetadata();

    wName.setText(Const.NVL(splunk.getName(), ""));
    wHostname.setText(Const.NVL(splunk.getHostname(), ""));
    wPort.setText(Const.NVL(splunk.getPort(), ""));
    wUsername.setText(Const.NVL(splunk.getUsername(), ""));
    wPassword.setText(Const.NVL(splunk.getPassword(), ""));

    wName.setFocus();
  }

  @Override
  public void getWidgetsContent(SplunkConnection splunk) {
    splunk.setName(wName.getText());
    splunk.setHostname(wHostname.getText());
    splunk.setPort(wPort.getText());
    splunk.setUsername(wUsername.getText());
    splunk.setPassword(wPassword.getText());
  }

  public void test() {
    SplunkConnection splunk = new SplunkConnection();
    IVariables variables = manager.getVariables();
    try {
      getWidgetsContent(splunk);
      splunk.test(variables);
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK);
      box.setText("OK");
      String message = "Connection successful!" + Const.CR;
      message += Const.CR;
      message +=
          "Hostname : "
              + splunk.getRealHostname(variables)
              + ", port : "
              + splunk.getRealPort(variables)
              + ", user : "
              + splunk.getRealUsername(variables);
      box.setMessage(message);
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error connecting to Splunk with Hostname '"
              + splunk.getRealHostname(variables)
              + "', port "
              + splunk.getRealPort(variables)
              + ", and username '"
              + splunk.getRealUsername(variables),
          e);
    }
  }
}
