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

package org.apache.hop.redis.metadata;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/** redis connection editor */
@SuppressWarnings("java:S2160")
public class RedisConnectionEditor extends MetadataEditor<RedisConnection> {

  private static final Class<?> PKG = RedisConnection.class;

  /** Widgets belonging to the Connection config group. */
  public static final String CONNECTION_WIDGET_ID = "RedisConnectionEditor.Connection.ParentId";

  /** Widgets belonging to the Pool config group (shared by all deployment modes). */
  public static final String POOL_WIDGET_ID = "RedisConnectionEditor.Pool.ParentId";

  private Composite parent;
  private Text wName;
  private GuiCompositeWidgets connectionWidgets;
  private GuiCompositeWidgets poolWidgets;
  private Group gConnection;
  private Group gPool;
  private ScrolledComposite wScrolled;
  private Composite wContent;

  public RedisConnectionEditor(
      HopGui hopGui, MetadataManager<RedisConnection> manager, RedisConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    this.parent = parent;

    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "RedisConnectionEditor.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin * 2);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // Vertical scroll only: multi-line node lists must not force a horizontal bar via pack().
    wScrolled = new ScrolledComposite(parent, SWT.V_SCROLL);
    FormData fdScrolled = new FormData();
    fdScrolled.left = new FormAttachment(0, 0);
    fdScrolled.right = new FormAttachment(100, 0);
    fdScrolled.top = new FormAttachment(spacer, 15);
    fdScrolled.bottom = new FormAttachment(100, 0);
    wScrolled.setLayoutData(fdScrolled);
    wScrolled.setExpandHorizontal(true);
    wScrolled.setExpandVertical(true);

    wContent = new Composite(wScrolled, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 0;
    contentLayout.marginHeight = 0;
    wContent.setLayout(contentLayout);
    wScrolled.setContent(wContent);

    gConnection = new Group(wContent, SWT.SHADOW_ETCHED_IN);
    PropsUi.setLook(gConnection);
    gConnection.setText(BaseMessages.getString(PKG, "RedisConnectionEditor.ConnectionGroup.Label"));
    FormLayout connectionLayout = new FormLayout();
    connectionLayout.marginWidth = 10;
    connectionLayout.marginHeight = 10;
    gConnection.setLayout(connectionLayout);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, 0);
    gConnection.setLayoutData(fdConnection);

    connectionWidgets = new GuiCompositeWidgets(manager.getVariables());
    connectionWidgets.createCompositeWidgets(
        getMetadata(), null, gConnection, CONNECTION_WIDGET_ID, null);

    gPool = new Group(wContent, SWT.SHADOW_ETCHED_IN);
    PropsUi.setLook(gPool);
    gPool.setText(BaseMessages.getString(PKG, "RedisConnectionEditor.PoolGroup.Label"));
    FormLayout poolLayout = new FormLayout();
    poolLayout.marginWidth = 10;
    poolLayout.marginHeight = 10;
    gPool.setLayout(poolLayout);
    FormData fdPool = new FormData();
    fdPool.left = new FormAttachment(0, 0);
    fdPool.right = new FormAttachment(100, 0);
    fdPool.top = new FormAttachment(gConnection, 15);
    gPool.setLayoutData(fdPool);

    poolWidgets = new GuiCompositeWidgets(manager.getVariables());
    poolWidgets.createCompositeWidgets(getMetadata(), null, gPool, POOL_WIDGET_ID, null);

    wScrolled.addListener(SWT.Resize, e -> relayoutScrolledContent());

    setWidgetsContent();
    updateVisibility();

    wName.addListener(SWT.Modify, e -> setChanged());
    GuiCompositeWidgetsAdapter listener =
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
            if (RedisConnection.WIDGET_ID_DEPLOYMENT_MODE.equals(widgetId)) {
              updateVisibility();
            }
          }
        };
    connectionWidgets.setWidgetsListener(listener);
    poolWidgets.setWidgetsListener(listener);
  }

  @Override
  public void setWidgetsContent() {
    RedisConnection meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    connectionWidgets.setWidgetsContents(meta, gConnection, CONNECTION_WIDGET_ID);
    poolWidgets.setWidgetsContents(meta, gPool, POOL_WIDGET_ID);
    updateVisibility();
  }

  @Override
  public void getWidgetsContent(RedisConnection meta) {
    meta.setName(wName.getText());
    connectionWidgets.getWidgetsContents(meta, CONNECTION_WIDGET_ID);
    poolWidgets.getWidgetsContents(meta, POOL_WIDGET_ID);
    if (meta.getDeploymentMode() == RedisDeploymentMode.CLUSTER) {
      meta.setDatabase("0");
    }
  }

  /**
   * Show only the Connection-config fields that apply to the selected deployment mode. Pool config
   * is always visible and shared across Standalone / Sentinel / Cluster.
   */
  private void updateVisibility() {
    if (connectionWidgets == null) {
      return;
    }
    RedisDeploymentMode mode = readDeploymentMode();
    Set<String> hidden = new HashSet<>();

    boolean standalone = mode == RedisDeploymentMode.STANDALONE;
    boolean sentinel = mode == RedisDeploymentMode.SENTINEL;
    boolean cluster = mode == RedisDeploymentMode.CLUSTER;

    hideUnless(
        hidden, standalone, RedisConnection.WIDGET_ID_HOSTNAME, RedisConnection.WIDGET_ID_PORT);
    hideUnless(
        hidden,
        sentinel,
        RedisConnection.WIDGET_ID_MASTER_NAME,
        RedisConnection.WIDGET_ID_SENTINEL_NODES);
    hideUnless(hidden, cluster, RedisConnection.WIDGET_ID_CLUSTER_NODES);
    hideUnless(hidden, standalone || sentinel, RedisConnection.WIDGET_ID_DATABASE);

    connectionWidgets.setWidgetsHidden(getMetadata(), hidden);

    if (gConnection != null && !gConnection.isDisposed()) {
      gConnection.layout(true, true);
    }
    if (gPool != null && !gPool.isDisposed()) {
      gPool.layout(true, true);
    }
    relayoutScrolledContent();
  }

  /**
   * Size the scrolled content to the client width so multi-line node fields (long host:port lines)
   * do not create a horizontal scrollbar between the name field and the button bar.
   */
  private void relayoutScrolledContent() {
    if (wScrolled == null || wScrolled.isDisposed() || wContent == null || wContent.isDisposed()) {
      return;
    }
    wContent.layout(true, true);
    Rectangle client = wScrolled.getClientArea();
    int width = Math.max(client.width, 1);
    Point size = wContent.computeSize(width, SWT.DEFAULT);
    wScrolled.setMinWidth(width);
    wScrolled.setMinHeight(size.y);
    wContent.setSize(width, size.y);
  }

  private void hideUnless(Set<String> hidden, boolean applies, String... widgetIds) {
    if (!applies) {
      hidden.addAll(List.of(widgetIds));
    }
  }

  private RedisDeploymentMode readDeploymentMode() {
    Control control =
        connectionWidgets.getWidgetsMap().get(RedisConnection.WIDGET_ID_DEPLOYMENT_MODE);
    if (control instanceof Combo combo) {
      return RedisDeploymentMode.fromCode(combo.getText());
    }
    RedisDeploymentMode mode = getMetadata().getDeploymentMode();
    return mode == null ? RedisDeploymentMode.STANDALONE : mode;
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    Button wbTest = new Button(parent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTest);
    wbTest.setText("Test");
    wbTest.addListener(SWT.Selection, e -> test());
    return new Button[] {wbTest};
  }

  public void test() {
    try {
      RedisConnection meta = new RedisConnection();
      getWidgetsContent(meta);
      meta.test(manager.getVariables());
      MessageBox box = new MessageBox(parent.getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText("Success!");
      box.setMessage(
          "Connected successfully (PING → PONG)!"
              + (meta.getDeploymentMode() == RedisDeploymentMode.CLUSTER
                  ? " Cluster topology and slot routing were also verified."
                  : ""));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), "Error", "We couldn't connect using this information", e);
    }
  }
}
