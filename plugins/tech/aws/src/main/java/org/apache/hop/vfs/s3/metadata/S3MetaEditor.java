/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.vfs.s3.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

@GuiPlugin(description = "Editor for S3 connection metadata")
public class S3MetaEditor extends MetadataEditor<S3Meta> {

  private static final Class<?> PKG = S3MetaEditor.class;

  /** Padding before the first widget and after the last widget inside each auth composite. */
  private static final int INSIDE_PADDING = 6;

  private PropsUi props;
  private int middle;
  private int margin;

  private Text wName;
  private TextVar wDescription;
  private Combo wRegion;
  private TextVar wPartSize;
  private TextVar wCacheTtl;

  private Label spacer;
  private Combo wAuthType;

  private Label wlAccessKey;
  private TextVar wAccessKey;
  private Label wlSecretKey;
  private TextVar wSecretKey;
  private Label wlSessionToken;
  private TextVar wSessionToken;

  private Label wlCredentialsFile;
  private TextVar wCredentialsFile;
  private Label wlProfileName;
  private TextVar wProfileName;

  private Label wlEndpoint;
  private TextVar wEndpoint;
  private Button wPathStyle;

  private Composite wAuthComposite;
  private Composite wKeysComposite;
  private Composite wProfileComposite;
  private Composite wCommonComposite;
  private FormData fdKeysComposite;
  private FormData fdProfileComposite;
  private FormData fdCommonComposite;

  public S3MetaEditor(HopGui hopGui, MetadataManager<S3Meta> manager, S3Meta metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    props = PropsUi.getInstance();
    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment(0, 0);
    fdlIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlIcon);
    PropsUi.setLook(wIcon);

    // Name row (same layout as DatabaseMetaEditor: label top=0, field below label, spacer below
    // field)
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "S3Meta.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, margin);
    fdName.left = new FormAttachment(0, 0);
    fdName.right = new FormAttachment(wIcon, -margin);
    wName.setLayoutData(fdName);

    spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // Content below spacer (15px gap, same as DatabaseMetaEditor)
    Control lastControl = spacer;

    // --- General: Description ---
    Label wlDesc = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDesc);
    wlDesc.setText(BaseMessages.getString(PKG, "S3Meta.Description.Label"));
    FormData fdlDesc = new FormData();
    fdlDesc.top = new FormAttachment(lastControl, 15);
    fdlDesc.left = new FormAttachment(0, 0);
    fdlDesc.right = new FormAttachment(middle, -margin);
    wlDesc.setLayoutData(fdlDesc);
    wDescription = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDesc = new FormData();
    fdDesc.top = new FormAttachment(wlDesc, 0, SWT.CENTER);
    fdDesc.left = new FormAttachment(middle, 0);
    fdDesc.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDesc);
    lastControl = wDescription;

    // --- General: Region (dropdown + editable) ---
    Label wlRegion = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlRegion);
    wlRegion.setText(BaseMessages.getString(PKG, "S3Meta.Region.Label"));
    FormData fdlRegion = new FormData();
    fdlRegion.top = new FormAttachment(lastControl, margin);
    fdlRegion.left = new FormAttachment(0, 0);
    fdlRegion.right = new FormAttachment(middle, -margin);
    wlRegion.setLayoutData(fdlRegion);
    wRegion = new Combo(parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT);
    PropsUi.setLook(wRegion);
    wRegion.setItems(AwsRegions.getRegionIds());
    FormData fdRegion = new FormData();
    fdRegion.top = new FormAttachment(wlRegion, 0, SWT.CENTER);
    fdRegion.left = new FormAttachment(middle, 0);
    fdRegion.right = new FormAttachment(100, 0);
    wRegion.setLayoutData(fdRegion);
    lastControl = wRegion;

    // --- General: Part size ---
    Label wlPartSize = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPartSize);
    wlPartSize.setText(BaseMessages.getString(PKG, "S3Meta.PartSize.Label"));
    FormData fdlPartSize = new FormData();
    fdlPartSize.top = new FormAttachment(lastControl, margin);
    fdlPartSize.left = new FormAttachment(0, 0);
    fdlPartSize.right = new FormAttachment(middle, -margin);
    wlPartSize.setLayoutData(fdlPartSize);
    wPartSize = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPartSize);
    FormData fdPartSize = new FormData();
    fdPartSize.top = new FormAttachment(wlPartSize, 0, SWT.CENTER);
    fdPartSize.left = new FormAttachment(middle, 0);
    fdPartSize.right = new FormAttachment(100, 0);
    wPartSize.setLayoutData(fdPartSize);
    lastControl = wPartSize;

    // --- General: Cache TTL ---
    Label wlCacheTtl = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlCacheTtl);
    wlCacheTtl.setText(BaseMessages.getString(PKG, "S3Meta.CacheTtl.Label"));
    FormData fdlCacheTtl = new FormData();
    fdlCacheTtl.top = new FormAttachment(lastControl, margin);
    fdlCacheTtl.left = new FormAttachment(0, 0);
    fdlCacheTtl.right = new FormAttachment(middle, -margin);
    wlCacheTtl.setLayoutData(fdlCacheTtl);
    wCacheTtl = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCacheTtl);
    FormData fdCacheTtl = new FormData();
    fdCacheTtl.top = new FormAttachment(wlCacheTtl, 0, SWT.CENTER);
    fdCacheTtl.left = new FormAttachment(middle, 0);
    fdCacheTtl.right = new FormAttachment(100, 0);
    wCacheTtl.setLayoutData(fdCacheTtl);
    lastControl = wCacheTtl;

    // --- Authentication type ---
    Label wlAuthType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlAuthType);
    wlAuthType.setText(BaseMessages.getString(PKG, "S3Meta.AuthType.Label"));
    FormData fdlAuthType = new FormData();
    fdlAuthType.top = new FormAttachment(lastControl, 15);
    fdlAuthType.left = new FormAttachment(0, 0);
    fdlAuthType.right = new FormAttachment(middle, -margin);
    wlAuthType.setLayoutData(fdlAuthType);
    wAuthType = new Combo(parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT | SWT.READ_ONLY);
    PropsUi.setLook(wAuthType);
    wAuthType.setItems(
        new String[] {
          BaseMessages.getString(PKG, "S3Meta.AuthType.Default"),
          BaseMessages.getString(PKG, "S3Meta.AuthType.AccessKeys"),
          BaseMessages.getString(PKG, "S3Meta.AuthType.CredentialsFile"),
          BaseMessages.getString(PKG, "S3Meta.AuthType.Anonymous"),
        });
    FormData fdAuthType = new FormData();
    fdAuthType.top = new FormAttachment(wlAuthType, 0, SWT.CENTER);
    fdAuthType.left = new FormAttachment(middle, 0);
    fdAuthType.right = new FormAttachment(100, 0);
    wAuthType.setLayoutData(fdAuthType);
    lastControl = wAuthType;

    // --- Auth-specific composite ---
    wAuthComposite = new Composite(parent, SWT.NONE);
    PropsUi.setLook(wAuthComposite);
    wAuthComposite.setLayout(new FormLayout());
    FormData fdAuthComp = new FormData();
    fdAuthComp.left = new FormAttachment(0, 0);
    fdAuthComp.right = new FormAttachment(100, 0);
    fdAuthComp.top = new FormAttachment(lastControl, margin);
    fdAuthComp.bottom = new FormAttachment(100, 0);
    wAuthComposite.setLayoutData(fdAuthComp);

    buildAuthWidgets(wAuthComposite);

    wAuthType.addListener(
        SWT.Selection,
        e -> {
          setChanged();
          updateAuthVisibility();
        });

    addModifyListeners();
    setWidgetsContent();
    resetChanged();
    updateAuthVisibility();
  }

  private void buildAuthWidgets(Composite comp) {
    // --- Keys block (Access key, Secret key, Session token) ---
    wKeysComposite = new Composite(comp, SWT.NONE);
    PropsUi.setLook(wKeysComposite);
    wKeysComposite.setLayout(new FormLayout());
    fdKeysComposite = new FormData();
    fdKeysComposite.top = new FormAttachment(0, 0);
    fdKeysComposite.left = new FormAttachment(0, 0);
    fdKeysComposite.right = new FormAttachment(100, 0);
    wKeysComposite.setLayoutData(fdKeysComposite);

    wlAccessKey = new Label(wKeysComposite, SWT.RIGHT);
    PropsUi.setLook(wlAccessKey);
    wlAccessKey.setText(BaseMessages.getString(PKG, "S3Meta.AccessKey.Label"));
    FormData fdl = new FormData();
    fdl.top = new FormAttachment(0, INSIDE_PADDING);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlAccessKey.setLayoutData(fdl);
    wAccessKey =
        new TextVar(
            manager.getVariables(),
            wKeysComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wAccessKey);
    FormData fd = new FormData();
    fd.top = new FormAttachment(wlAccessKey, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wAccessKey.setLayoutData(fd);

    wlSecretKey = new Label(wKeysComposite, SWT.RIGHT);
    PropsUi.setLook(wlSecretKey);
    wlSecretKey.setText(BaseMessages.getString(PKG, "S3Meta.SecretKey.Label"));
    fdl = new FormData();
    fdl.top = new FormAttachment(wAccessKey, margin);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlSecretKey.setLayoutData(fdl);
    wSecretKey =
        new TextVar(
            manager.getVariables(),
            wKeysComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wSecretKey);
    fd = new FormData();
    fd.top = new FormAttachment(wlSecretKey, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wSecretKey.setLayoutData(fd);

    wlSessionToken = new Label(wKeysComposite, SWT.RIGHT);
    PropsUi.setLook(wlSessionToken);
    wlSessionToken.setText(BaseMessages.getString(PKG, "S3Meta.SessionToken.Label"));
    fdl = new FormData();
    fdl.top = new FormAttachment(wSecretKey, margin);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlSessionToken.setLayoutData(fdl);
    wSessionToken =
        new TextVar(
            manager.getVariables(),
            wKeysComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    PropsUi.setLook(wSessionToken);
    fd = new FormData();
    fd.top = new FormAttachment(wlSessionToken, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wSessionToken.setLayoutData(fd);

    // --- Profile block (Credentials file, Profile name) ---
    wProfileComposite = new Composite(comp, SWT.NONE);
    PropsUi.setLook(wProfileComposite);
    wProfileComposite.setLayout(new FormLayout());
    fdProfileComposite = new FormData();
    fdProfileComposite.top = new FormAttachment(0, 0);
    fdProfileComposite.left = new FormAttachment(0, 0);
    fdProfileComposite.right = new FormAttachment(100, 0);
    wProfileComposite.setLayoutData(fdProfileComposite);

    wlCredentialsFile = new Label(wProfileComposite, SWT.RIGHT);
    PropsUi.setLook(wlCredentialsFile);
    wlCredentialsFile.setText(BaseMessages.getString(PKG, "S3Meta.CredentialsFile.Label"));
    fdl = new FormData();
    fdl.top = new FormAttachment(0, INSIDE_PADDING);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlCredentialsFile.setLayoutData(fdl);
    wCredentialsFile =
        new TextVar(manager.getVariables(), wProfileComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCredentialsFile);
    fd = new FormData();
    fd.top = new FormAttachment(wlCredentialsFile, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wCredentialsFile.setLayoutData(fd);

    wlProfileName = new Label(wProfileComposite, SWT.RIGHT);
    PropsUi.setLook(wlProfileName);
    wlProfileName.setText(BaseMessages.getString(PKG, "S3Meta.ProfileName.Label"));
    fdl = new FormData();
    fdl.top = new FormAttachment(wCredentialsFile, margin);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlProfileName.setLayoutData(fdl);
    wProfileName =
        new TextVar(manager.getVariables(), wProfileComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProfileName);
    fd = new FormData();
    fd.top = new FormAttachment(wlProfileName, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wProfileName.setLayoutData(fd);

    // --- Common block (Endpoint, Path style) - position updated in updateAuthVisibility ---
    wCommonComposite = new Composite(comp, SWT.NONE);
    PropsUi.setLook(wCommonComposite);
    wCommonComposite.setLayout(new FormLayout());
    fdCommonComposite = new FormData();
    fdCommonComposite.left = new FormAttachment(0, 0);
    fdCommonComposite.right = new FormAttachment(100, 0);
    wCommonComposite.setLayoutData(fdCommonComposite);

    wlEndpoint = new Label(wCommonComposite, SWT.RIGHT);
    PropsUi.setLook(wlEndpoint);
    wlEndpoint.setText(BaseMessages.getString(PKG, "S3Meta.Endpoint.Label"));
    fdl = new FormData();
    fdl.top = new FormAttachment(0, INSIDE_PADDING);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlEndpoint.setLayoutData(fdl);
    wEndpoint =
        new TextVar(manager.getVariables(), wCommonComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEndpoint);
    fd = new FormData();
    fd.top = new FormAttachment(wlEndpoint, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wEndpoint.setLayoutData(fd);

    Label wlPathStyle = new Label(wCommonComposite, SWT.RIGHT);
    PropsUi.setLook(wlPathStyle);
    wlPathStyle.setText(BaseMessages.getString(PKG, "S3Meta.PathStyle.Label"));
    fdl = new FormData();
    fdl.top = new FormAttachment(wEndpoint, margin);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    wlPathStyle.setLayoutData(fdl);
    wPathStyle = new Button(wCommonComposite, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wPathStyle);
    fd = new FormData();
    fd.top = new FormAttachment(wlPathStyle, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wPathStyle.setLayoutData(fd);
    // Small spacer below Path-style (padding after last widget inside composite)
    Label bottomSpacer = new Label(wCommonComposite, SWT.NONE);
    FormData fdSpacer = new FormData();
    fdSpacer.top = new FormAttachment(wPathStyle, margin);
    fdSpacer.bottom = new FormAttachment(wPathStyle, margin + INSIDE_PADDING);
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.right = new FormAttachment(100, 0);
    bottomSpacer.setLayoutData(fdSpacer);
  }

  private void updateAuthVisibility() {
    int idx = wAuthType.getSelectionIndex();
    if (idx < 0) {
      idx = 0;
    }
    boolean keys = (idx == 1);
    boolean profile = (idx == 2);
    wKeysComposite.setVisible(keys);
    wProfileComposite.setVisible(profile);
    // Collapse hidden composites so they take no space (FormLayout reserves space for invisible
    // controls otherwise)
    if (keys) {
      fdKeysComposite.top = new FormAttachment(0, 0);
      fdKeysComposite.bottom = null;
    } else {
      fdKeysComposite.top = new FormAttachment(0, 0);
      fdKeysComposite.bottom = new FormAttachment(0, 0);
    }
    if (profile) {
      fdProfileComposite.top = new FormAttachment(0, 0);
      fdProfileComposite.bottom = null;
    } else {
      fdProfileComposite.top = new FormAttachment(0, 0);
      fdProfileComposite.bottom = new FormAttachment(0, 0);
    }
    wKeysComposite.setLayoutData(fdKeysComposite);
    wProfileComposite.setLayoutData(fdProfileComposite);
    if (keys) {
      fdCommonComposite.top = new FormAttachment(wKeysComposite, margin);
    } else if (profile) {
      fdCommonComposite.top = new FormAttachment(wProfileComposite, margin);
    } else {
      fdCommonComposite.top = new FormAttachment(0, 0);
    }
    wCommonComposite.setLayoutData(fdCommonComposite);
    wAuthComposite.layout(true, true);
  }

  private void addModifyListeners() {
    wName.addListener(SWT.Modify, e -> setChanged());
    wDescription.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wRegion.addListener(SWT.Modify, e -> setChanged());
    wPartSize.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wCacheTtl.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wAccessKey.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wSecretKey.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wSessionToken.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wCredentialsFile.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wProfileName.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wEndpoint.getTextWidget().addListener(SWT.Modify, e -> setChanged());
    wPathStyle.addListener(SWT.Selection, e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    S3Meta meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    wDescription.setText(Const.NVL(meta.getDescription(), ""));
    String region = Const.NVL(meta.getRegion(), "");
    wRegion.setText(region);
    if (wRegion.indexOf(region) < 0 && StringUtils.isNotEmpty(region)) {
      wRegion.add(region, 0);
      wRegion.select(0);
    } else if (wRegion.indexOf(region) >= 0) {
      wRegion.select(wRegion.indexOf(region));
    }
    wPartSize.setText(Const.NVL(meta.getPartSize(), "5MB"));
    wCacheTtl.setText(Const.NVL(meta.getCacheTtlSeconds(), "5"));

    String authType = meta.getAuthenticationType();
    if (StringUtils.isEmpty(authType)) {
      authType = inferAuthType(meta);
    }
    authType = Const.NVL(authType, S3AuthType.DEFAULT.name());
    int authIdx = 0;
    if (S3AuthType.ACCESS_KEYS.name().equals(authType)) {
      authIdx = 1;
    } else if (S3AuthType.CREDENTIALS_FILE.name().equals(authType)) {
      authIdx = 2;
    } else if (S3AuthType.ANONYMOUS.name().equals(authType)) {
      authIdx = 3;
    }
    wAuthType.select(authIdx);

    wAccessKey.setText(Const.NVL(meta.getAccessKey(), ""));
    wSecretKey.setText(Const.NVL(meta.getSecretKey(), ""));
    wSessionToken.setText(Const.NVL(meta.getSessionToken(), ""));
    wCredentialsFile.setText(Const.NVL(meta.getCredentialsFile(), ""));
    wProfileName.setText(Const.NVL(meta.getProfileName(), ""));
    wEndpoint.setText(Const.NVL(meta.getEndpoint(), ""));
    wPathStyle.setSelection(meta.isPathStyleAccess());
    updateAuthVisibility();
  }

  @Override
  public void getWidgetsContent(S3Meta meta) {
    meta.setName(wName.getText());
    meta.setDescription(wDescription.getText());
    meta.setRegion(StringUtils.isNotEmpty(wRegion.getText()) ? wRegion.getText().trim() : null);
    meta.setPartSize(wPartSize.getText());
    meta.setCacheTtlSeconds(wCacheTtl.getText());

    int authIdx = wAuthType.getSelectionIndex();
    if (authIdx == 1) {
      meta.setAuthenticationType(S3AuthType.ACCESS_KEYS.name());
      meta.setAccessKey(wAccessKey.getText());
      meta.setSecretKey(wSecretKey.getText());
      meta.setSessionToken(wSessionToken.getText());
      meta.setCredentialsFile(null);
      meta.setProfileName(null);
    } else if (authIdx == 2) {
      meta.setAuthenticationType(S3AuthType.CREDENTIALS_FILE.name());
      meta.setAccessKey(null);
      meta.setSecretKey(null);
      meta.setSessionToken(null);
      meta.setCredentialsFile(wCredentialsFile.getText());
      meta.setProfileName(wProfileName.getText());
    } else if (authIdx == 3) {
      meta.setAuthenticationType(S3AuthType.ANONYMOUS.name());
      meta.setAccessKey(null);
      meta.setSecretKey(null);
      meta.setSessionToken(null);
      meta.setCredentialsFile(null);
      meta.setProfileName(null);
    } else {
      meta.setAuthenticationType(S3AuthType.DEFAULT.name());
      meta.setAccessKey(null);
      meta.setSecretKey(null);
      meta.setSessionToken(null);
      meta.setCredentialsFile(null);
      meta.setProfileName(wProfileName.getText());
    }
    meta.setEndpoint(wEndpoint.getText());
    meta.setPathStyleAccess(wPathStyle.getSelection());
  }

  @Override
  public boolean setFocus() {
    return wName != null && !wName.isDisposed() && wName.setFocus();
  }

  private static String inferAuthType(S3Meta meta) {
    if (StringUtils.isNotEmpty(meta.getAccessKey())
        || StringUtils.isNotEmpty(meta.getSecretKey())) {
      return S3AuthType.ACCESS_KEYS.name();
    }
    if (StringUtils.isNotEmpty(meta.getCredentialsFile())) {
      return S3AuthType.CREDENTIALS_FILE.name();
    }
    return S3AuthType.DEFAULT.name();
  }

  @Override
  public void save() throws HopException {
    super.save();
    HopVfs.reset();
  }
}
