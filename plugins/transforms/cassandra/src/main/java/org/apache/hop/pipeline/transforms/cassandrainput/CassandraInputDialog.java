/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.pipeline.transforms.cassandrainput;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.cassandra.ConnectionFactory;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transforms.tableinput.SqlValuesHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog class for the CassandraInput step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = CassandraInputMeta.class;

  private final CassandraInputMeta m_currentMeta;
  private final CassandraInputMeta m_originalMeta;

  /** various UI bits and pieces for the dialog */
  private Label m_transformNameLabel;

  private Text m_transformNameText;

  private Label m_hostLab;
  private TextVar m_hostText;
  private Label m_portLab;
  private TextVar m_portText;

  private Label m_userLab;
  private TextVar m_userText;
  private Label m_passLab;
  private TextVar m_passText;

  private Label m_keyspaceLab;
  private TextVar m_keyspaceText;

  private Label m_compressionLab;
  private Button m_useCompressionBut;

  private Label m_timeoutLab;
  private TextVar m_timeoutText;

  private Label m_maxLengthLab;
  private TextVar m_maxLengthText;

  private Label m_positionLab;

  private Button m_showSchemaBut;

  private Label m_cqlLab;
  private StyledTextComp m_cqlText;

  private Button m_executeForEachRowBut;

  public CassandraInputDialog(Shell parent, Object in, PipelineMeta tr, String name) {

    super(parent, (BaseTransformMeta) in, tr, name);

    m_currentMeta = (CassandraInputMeta) in;
    m_originalMeta = (CassandraInputMeta) m_currentMeta.clone();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    props.setLook(shell);
    setShellImage(shell, m_currentMeta);

    // used to listen to a text field (m_wtransformName)
    ModifyListener lsMod =
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            m_currentMeta.setChanged();
          }
        };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "CassandraInputDialog.Shell.Title")); // $NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // transformName line
    m_transformNameLabel = new Label(shell, SWT.RIGHT);
    m_transformNameLabel.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.transformName.Label")); // $NON-NLS-1$
    props.setLook(m_transformNameLabel);

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(0, margin);
    m_transformNameLabel.setLayoutData(fd);
    m_transformNameText = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    // m_transformNameText.setText( transformName );
    props.setLook(m_transformNameText);
    m_transformNameText.addModifyListener(lsMod);

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(100, 0);
    m_transformNameText.setLayoutData(fd);

    // host line
    m_hostLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_hostLab);
    m_hostLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.Hostname.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_transformNameText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_hostLab.setLayoutData(fd);

    m_hostText = new TextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_hostText);
    m_hostText.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            m_hostText.setToolTipText(pipelineMeta.environmentSubstitute(m_hostText.getText()));
          }
        });
    m_hostText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_transformNameText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_hostText.setLayoutData(fd);

    // port line
    m_portLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_portLab);
    m_portLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.Port.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_hostText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_portLab.setLayoutData(fd);

    m_portText = new TextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_portText);
    m_portText.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            m_portText.setToolTipText(pipelineMeta.environmentSubstitute(m_portText.getText()));
          }
        });
    m_portText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_hostText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_portText.setLayoutData(fd);

    // timeout line
    m_timeoutLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_timeoutLab);
    m_timeoutLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.Timeout.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_portText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_timeoutLab.setLayoutData(fd);

    m_timeoutText = new TextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_timeoutText);
    m_timeoutText.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            m_timeoutText.setToolTipText(
                pipelineMeta.environmentSubstitute(m_timeoutText.getText()));
          }
        });
    m_timeoutText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_portText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_timeoutText.setLayoutData(fd);

    // max length line
    m_maxLengthLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_maxLengthLab);
    m_maxLengthLab.setText(
        BaseMessages.getString(
            PKG, "CassandraInputDialog.TransportMaxLength.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_timeoutText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_maxLengthLab.setLayoutData(fd);

    m_maxLengthText = new TextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_maxLengthText);
    m_maxLengthText.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            m_maxLengthText.setToolTipText(
                pipelineMeta.environmentSubstitute(m_maxLengthText.getText()));
          }
        });
    m_maxLengthText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_timeoutText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_maxLengthText.setLayoutData(fd);

    // username line
    m_userLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_userLab);
    m_userLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.User.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_maxLengthText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_userLab.setLayoutData(fd);

    m_userText = new TextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_userText);
    m_userText.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_maxLengthText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_userText.setLayoutData(fd);

    // password line
    m_passLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_passLab);
    m_passLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.Password.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_userText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_passLab.setLayoutData(fd);

    m_passText = new PasswordTextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_passText);
    m_passText.addModifyListener(lsMod);

    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_userText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_passText.setLayoutData(fd);

    // keyspace line
    m_keyspaceLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_keyspaceLab);
    m_keyspaceLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.Keyspace.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_passText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_keyspaceLab.setLayoutData(fd);

    m_keyspaceText = new TextVar(pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(m_keyspaceText);
    m_keyspaceText.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            m_keyspaceText.setToolTipText(
                pipelineMeta.environmentSubstitute(m_keyspaceText.getText()));
          }
        });
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(m_passText, margin);
    fd.left = new FormAttachment(middle, 0);
    m_keyspaceText.setLayoutData(fd);

    // compression check box
    m_compressionLab = new Label(shell, SWT.RIGHT);
    props.setLook(m_compressionLab);
    m_compressionLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.UseCompression.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_keyspaceText, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_compressionLab.setLayoutData(fd);

    m_useCompressionBut = new Button(shell, SWT.CHECK);
    props.setLook(m_useCompressionBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(m_keyspaceText, margin);
    m_useCompressionBut.setLayoutData(fd);
    m_useCompressionBut.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            m_currentMeta.setChanged();
          }
        });

    // execute for each row
    Label executeForEachLab = new Label(shell, SWT.RIGHT);
    props.setLook(executeForEachLab);
    executeForEachLab.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.ExecuteForEachRow.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment(middle, -margin);
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_useCompressionBut, margin);
    executeForEachLab.setLayoutData(fd);

    m_executeForEachRowBut = new Button(shell, SWT.CHECK);
    props.setLook(m_executeForEachRowBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(m_useCompressionBut, margin);
    m_executeForEachRowBut.setLayoutData(fd);

    // Buttons inherited from BaseTransformDialog
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK")); // $NON-NLS-1$
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview")); // $NON-NLS-1$

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); // $NON-NLS-1$

    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, m_cqlText);

    // position label
    m_positionLab = new Label(shell, SWT.NONE);
    props.setLook(m_positionLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.bottom = new FormAttachment(wOk, -margin);
    m_positionLab.setLayoutData(fd);

    m_showSchemaBut = new Button(shell, SWT.PUSH);
    m_showSchemaBut.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.Schema.Button")); // $NON-NLS-1$
    props.setLook(m_showSchemaBut);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(wOk, -margin);
    m_showSchemaBut.setLayoutData(fd);

    m_showSchemaBut.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            popupSchemaInfo();
          }
        });

    // cql stuff
    m_cqlLab = new Label(shell, SWT.NONE);
    props.setLook(m_cqlLab);
    m_cqlLab.setText(BaseMessages.getString(PKG, "CassandraInputDialog.CQL.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_executeForEachRowBut, margin);
    fd.right = new FormAttachment(middle, -margin);
    m_cqlLab.setLayoutData(fd);

    m_cqlText =
        new StyledTextComp(
            pipelineMeta,
            shell,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
            ""); //$NON-NLS-1$
    props.setLook(m_cqlText, Props.WIDGET_STYLE_FIXED);
    m_cqlText.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(m_cqlLab, margin);
    fd.right = new FormAttachment(100, -2 * margin);
    fd.bottom = new FormAttachment(m_showSchemaBut, -margin);
    m_cqlText.setLayoutData(fd);
    m_cqlText.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            setPosition();
            m_cqlText.setToolTipText(pipelineMeta.environmentSubstitute(m_cqlText.getText()));
          }
        });

    // Text Highlighting
    m_cqlText.addLineStyleListener(new SqlValuesHighlight());

    m_cqlText.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });

    m_cqlText.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });

    m_cqlText.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    // Add listeners
    lsCancel =
        new Listener() {
          @Override
          public void handleEvent(Event e) {
            cancel();
          }
        };

    lsOk =
        new Listener() {
          @Override
          public void handleEvent(Event e) {
            ok();
          }
        };

    lsPreview =
        new Listener() {
          @Override
          public void handleEvent(Event e) {
            preview();
          }
        };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOk.addListener(SWT.Selection, lsOk);
    wPreview.addListener(SWT.Selection, lsPreview);

    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    m_transformNameText.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    setSize();

    getData();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return transformName;
  }

  private void getInfo(CassandraInputMeta meta) {
    meta.setCassandraHost(m_hostText.getText());
    meta.setCassandraPort(m_portText.getText());
    meta.setSocketTimeout(m_timeoutText.getText());
    meta.setMaxLength(m_maxLengthText.getText());
    meta.setUsername(m_userText.getText());
    meta.setPassword(m_passText.getText());
    meta.setCassandraKeyspace(m_keyspaceText.getText());
    meta.setUseCompression(m_useCompressionBut.getSelection());
    meta.setCQLSelectQuery(m_cqlText.getText());
    meta.setExecuteForEachIncomingRow(m_executeForEachRowBut.getSelection());
  }

  protected void ok() {
    if (Utils.isEmpty(m_transformNameText.getText())) {
      return;
    }

    transformName = m_transformNameText.getText();
    getInfo(m_currentMeta);

    if (!m_originalMeta.equals(m_currentMeta)) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }

  protected void cancel() {
    transformName = null;
    m_currentMeta.setChanged(changed);

    dispose();
  }

  protected void getData() {
    if (!Utils.isEmpty(m_currentMeta.getCassandraHost())) {
      m_hostText.setText(m_currentMeta.getCassandraHost());
    }

    if (!Utils.isEmpty(m_currentMeta.getCassandraPort())) {
      m_portText.setText(m_currentMeta.getCassandraPort());
    }

    if (!Utils.isEmpty(m_currentMeta.getSocketTimeout())) {
      m_timeoutText.setText(m_currentMeta.getSocketTimeout());
    }

    if (!Utils.isEmpty(m_currentMeta.getMaxLength())) {
      m_maxLengthText.setText(m_currentMeta.getMaxLength());
    }

    if (!Utils.isEmpty(m_currentMeta.getUsername())) {
      m_userText.setText(m_currentMeta.getUsername());
    }

    if (!Utils.isEmpty(m_currentMeta.getPassword())) {
      m_passText.setText(m_currentMeta.getPassword());
    }

    if (!Utils.isEmpty(m_currentMeta.getCassandraKeyspace())) {
      m_keyspaceText.setText(m_currentMeta.getCassandraKeyspace());
    }

    m_useCompressionBut.setSelection(m_currentMeta.getUseCompression());
    m_executeForEachRowBut.setSelection(m_currentMeta.getExecuteForEachIncomingRow());

    if (!Utils.isEmpty(m_currentMeta.getCQLSelectQuery())) {
      m_cqlText.setText(m_currentMeta.getCQLSelectQuery());
    }
  }

  protected void setPosition() {
    String scr = m_cqlText.getText();
    int linenr = m_cqlText.getLineAtOffset(m_cqlText.getCaretOffset()) + 1;
    int posnr = m_cqlText.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while (posnr > 0 && scr.charAt(posnr - 1) != '\n' && scr.charAt(posnr - 1) != '\r') {
      posnr--;
      colnr++;
    }
    m_positionLab.setText(
        BaseMessages.getString(
            PKG,
            "CassandraInputDialog.Position.Label",
            "" + linenr,
            "" + colnr)); // $NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
  }

  private boolean checkForUnresolved(CassandraInputMeta meta, String title) {
    String query = pipelineMeta.environmentSubstitute(meta.getCQLSelectQuery());

    boolean notOk = (query.contains("${") || query.contains("?{")); // $NON-NLS-1$ //$NON-NLS-2$

    if (notOk) {
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              title,
              BaseMessages.getString(
                  PKG,
                  "CassandraInputDialog.Warning.Message.CassandraQueryContainsUnresolvedVarsFieldSubs")); //$NON-NLS-1$
      smd.open();
    }

    return !notOk;
  }

  private void preview() {
    CassandraInputMeta oneMeta = new CassandraInputMeta();
    getInfo(oneMeta);

    // Turn off execute for each incoming row (if set). Query is still going to
    // be stuffed if the user has specified field replacement (i.e. ?{...}) in
    // the query string
    oneMeta.setExecuteForEachIncomingRow(false);

    if (!checkForUnresolved(
        oneMeta,
        BaseMessages.getString(
            PKG,
            "CassandraInputDialog.Warning.Message.CassandraQueryContainsUnresolvedVarsFieldSubs.PreviewTitle"))) {
      return;
    }

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            pipelineMeta,
            pipelineMeta.getMetadataProvider(),
            oneMeta,
            m_transformNameText.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(
                PKG, "CassandraInputDialog.PreviewSize.DialogTitle"), // $NON-NLS-1$
            BaseMessages.getString(
                PKG, "CassandraInputDialog.PreviewSize.DialogMessage")); // $NON-NLS-1$

    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              previewMeta,
              new String[] {m_transformNameText.getText()},
              new int[] {previewSize});
      progressDialog.open();

      Pipeline trans = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()) {
        if (trans.getResult() != null && trans.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"), // $NON-NLS-1$
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"), // $NON-NLS-1$
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }
      }
      PreviewRowsDialog prd =
          new PreviewRowsDialog(
              shell,
              pipelineMeta,
              SWT.NONE,
              m_transformNameText.getText(),
              progressDialog.getPreviewRowsMeta(m_transformNameText.getText()),
              progressDialog.getPreviewRows(m_transformNameText.getText()),
              loggingText);
      prd.open();
    }
  }

  protected void popupSchemaInfo() {

    Connection conn = null;
    Keyspace kSpace = null;
    try {
      String hostS = pipelineMeta.environmentSubstitute(m_hostText.getText());
      String portS = pipelineMeta.environmentSubstitute(m_portText.getText());
      String userS = m_userText.getText();
      String passS = m_passText.getText();
      if (!Utils.isEmpty(userS) && !Utils.isEmpty(passS)) {
        userS = pipelineMeta.environmentSubstitute(userS);
        passS = pipelineMeta.environmentSubstitute(passS);
      }
      String keyspaceS = pipelineMeta.environmentSubstitute(m_keyspaceText.getText());
      String cqlText = pipelineMeta.environmentSubstitute(m_cqlText.getText());

      try {
        Map<String, String> opts = new HashMap<String, String>();
        opts.put(
            CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION,
            CassandraUtils.CQLOptions.CQL3_STRING);

        conn =
            CassandraUtils.getCassandraConnection(
                hostS,
                Integer.parseInt(portS),
                userS,
                passS,
                ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL,
                opts);

        conn.setHosts(hostS);
        conn.setDefaultPort(Integer.parseInt(portS));
        conn.setUsername(userS);
        conn.setPassword(passS);
        kSpace = conn.getKeyspace(keyspaceS);
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                    PKG,
                    "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message") //$NON-NLS-1$
                + ":\n\n"
                + e.getLocalizedMessage(),
            e); //$NON-NLS-1$
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title"), // $NON-NLS-1$
            BaseMessages.getString(
                    PKG,
                    "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message") //$NON-NLS-1$
                + ":\n\n"
                + e.getLocalizedMessage(),
            e); //$NON-NLS-1$
        return;
      }

      String table = CassandraUtils.getTableNameFromCQLSelectQuery(cqlText);
      if (Utils.isEmpty(table)) {
        throw new Exception(
            BaseMessages.getString(PKG, "CassandraInput.Error.NoFromClauseInQuery")); // $NON-NLS-1$
      }

      if (!kSpace.tableExists(table)) {
        throw new Exception(
            BaseMessages.getString(
                PKG,
                "CassandraInput.Error.NonExistentTable",
                CassandraUtils.removeQuotes(table),
                keyspaceS)); //$NON-NLS-1$
      }

      String schemaDescription = kSpace.getTableMetaData(table).describe();
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_INFORMATION | SWT.OK,
              "Schema info",
              schemaDescription,
              true); //$NON-NLS-1$
      smd.open();
    } catch (Exception e1) {
      logError(
          BaseMessages.getString(
                  PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message") // $NON-NLS-1$
              + ":\n\n"
              + e1.getMessage(),
          e1); //$NON-NLS-1$
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title"), // $NON-NLS-1$
          BaseMessages.getString(
                  PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message") // $NON-NLS-1$
              + ":\n\n"
              + e1.getMessage(),
          e1); //$NON-NLS-1$
    } finally {
      if (conn != null) {
        try {
          conn.closeConnection();
        } catch (Exception e) {
          log.logError(e.getLocalizedMessage(), e);
          // TODO popup another error dialog
        }
      }
    }
  }
}
