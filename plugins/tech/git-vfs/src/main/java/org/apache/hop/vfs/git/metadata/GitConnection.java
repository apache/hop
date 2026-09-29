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
package org.apache.hop.vfs.git.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.IVfsBrowseLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.vfs.git.GitAuthType;
import org.apache.hop.vfs.git.GitCheckout;
import org.apache.hop.vfs.git.GitCheckoutException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

/**
 * A named git repository, checked out at a branch or a revision and then read as an ordinary
 * folder. The name of the connection doubles as a VFS scheme: a connection called {@code ops} makes
 * every file of that repository available as {@code ops:///workflows/daily.hwf} in any transform,
 * action or dialog which accepts a file name.
 *
 * <p>This is what makes it possible to run {@code hop-run.sh my-git:///workflows/daily.hwf} from a
 * container which has nothing of the project on its disk but this one connection.
 *
 * <p>Credentials live here and are applied when the repository is fetched. They are never part of
 * the URI.
 */
@Getter
@Setter
@GuiPlugin(classLoaderGroup = "vfs-git")
@HopMetadata(
    key = "git-vfs-connection",
    name = "i18n::GitConnection.Name",
    description = "i18n::GitConnection.Description",
    image = "git.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/git-vfs-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_GIT_CONNECTION,
    supportsGlobalReplace = true,
    classLoaderGroup = "vfs-git")
public class GitConnection extends HopMetadataBase
    implements Serializable, IHopMetadata, IVfsBrowseLocation {

  private static final Class<?> PKG = GitConnection.class;

  public static final String GROUP_REPOSITORY = "Repository";
  public static final String GROUP_AUTHENTICATION = "Authentication";
  public static final String GROUP_ADVANCED = "Advanced";

  public static final String WIDGET_AUTH_TYPE = "20000-auth-type";
  public static final String WIDGET_USER_NAME = "20100-user-name";
  public static final String WIDGET_PASSWORD = "20200-password";
  public static final String WIDGET_PRIVATE_KEY = "20300-private-key";
  public static final String WIDGET_PASSPHRASE = "20400-passphrase";
  public static final String WIDGET_SSH_USER = "20500-ssh-user";
  public static final String WIDGET_KNOWN_HOSTS = "20600-known-hosts";
  public static final String WIDGET_ACCEPT_UNKNOWN_HOSTS = "20700-accept-unknown-hosts";

  @GuiWidgetElement(
      id = "10000-description",
      order = "0100",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.Description.Label",
      toolTip = "i18n::GitConnection.Description.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "010")
  @HopMetadataProperty
  private String description;

  /**
   * The repository to fetch: an HTTPS URL, an SSH URL, a {@code git://} URL or a path on disk.
   *
   * <p>The scheme decides which authentication method the server will accept, so it also decides
   * which authentication types are worth offering for it.
   */
  @GuiWidgetElement(
      id = "10100-repository-url",
      order = "0200",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.RepositoryUrl.Label",
      toolTip = "i18n::GitConnection.RepositoryUrl.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "010")
  @HopMetadataProperty
  private String repositoryUrl;

  /**
   * The branch, tag or commit to read, for example {@code main}, {@code v2.20.0} or a commit id.
   *
   * <p>Empty means whatever the remote calls its default branch. A commit id is the only choice
   * which cannot change under you, so it is what a deployment should point at.
   */
  @GuiWidgetElement(
      id = "10200-revision",
      order = "0300",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.Revision.Label",
      toolTip = "i18n::GitConnection.Revision.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "010")
  @HopMetadataProperty
  private String revision;

  /**
   * A folder inside the repository to treat as the root, for example {@code pipelines}. The
   * connection {@code ops} with a base path of {@code pipelines} serves {@code ops:///daily.hwf}
   * rather than {@code ops:///pipelines/daily.hwf}.
   */
  @GuiWidgetElement(
      id = "10300-base-path",
      order = "0400",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.BasePath.Label",
      toolTip = "i18n::GitConnection.BasePath.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "010")
  @HopMetadataProperty
  private String basePath;

  @GuiWidgetElement(
      id = "10400-test",
      order = "0450",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::GitConnection.Test.Label",
      toolTip = "i18n::GitConnection.Test.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_REPOSITORY,
      groupOrder = "010")
  public void testConnection(Object object) {
    GitConnection connection = (GitConnection) object;
    HopGui hopGui = HopGui.getInstance();
    IVariables variables = hopGui.getVariables();
    String title = BaseMessages.getString(PKG, "GitConnection.Test.Title");
    try {
      String result = new GitCheckout(variables, connection).probe();
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(title);
      box.setMessage(result);
      box.open();
    } catch (GitCheckoutException e) {
      new ErrorDialog(
          hopGui.getShell(),
          title,
          BaseMessages.getString(PKG, "GitConnection.Test.Error.Message"),
          e);
    }
  }

  @GuiWidgetElement(
      id = WIDGET_AUTH_TYPE,
      order = "0500",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::GitConnection.AuthType.Label",
      toolTip = "i18n::GitConnection.AuthType.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private GitAuthType authType = GitAuthType.NONE;

  @GuiWidgetElement(
      id = WIDGET_USER_NAME,
      order = "0600",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.UserName.Label",
      toolTip = "i18n::GitConnection.UserName.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String userName;

  /** A password, or the personal access token of a server which takes one in its place. */
  @GuiWidgetElement(
      id = WIDGET_PASSWORD,
      order = "0700",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::GitConnection.Password.Label",
      toolTip = "i18n::GitConnection.Password.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty(password = true)
  private String password;

  /** The private key of a deploy key, in the OpenSSH or PKCS#8 format JGit reads. */
  @GuiWidgetElement(
      id = WIDGET_PRIVATE_KEY,
      order = "0800",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::GitConnection.PrivateKey.Label",
      toolTip = "i18n::GitConnection.PrivateKey.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String privateKeyFile;

  /** The passphrase of the private key, empty when the key has none. */
  @GuiWidgetElement(
      id = WIDGET_PASSPHRASE,
      order = "0900",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::GitConnection.Passphrase.Label",
      toolTip = "i18n::GitConnection.Passphrase.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty(password = true)
  private String privateKeyPassphrase;

  /** The user to log in as over SSH, empty to take it from the repository URL. */
  @GuiWidgetElement(
      id = WIDGET_SSH_USER,
      order = "0950",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.SshUser.Label",
      toolTip = "i18n::GitConnection.SshUser.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String sshUser;

  /** A {@code known_hosts} file. Empty uses {@code ~/.ssh/known_hosts}. */
  @GuiWidgetElement(
      id = WIDGET_KNOWN_HOSTS,
      order = "0960",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::GitConnection.KnownHosts.Label",
      toolTip = "i18n::GitConnection.KnownHosts.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String knownHostsFile;

  /**
   * Connect even when the server's host key is not in the known hosts file.
   *
   * <p>A fresh container has no {@code known_hosts}. Leaving this off, the default, refuses the
   * connection until the host key is known.
   */
  @GuiWidgetElement(
      id = WIDGET_ACCEPT_UNKNOWN_HOSTS,
      order = "0970",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::GitConnection.AcceptUnknownHosts.Label",
      toolTip = "i18n::GitConnection.AcceptUnknownHosts.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean acceptUnknownHosts;

  /**
   * Where to keep the checkout this connection reads from.
   *
   * <p>A git repository is not a folder of files that can be read one at a time: the bytes of a
   * file only exist once git has put the revision together on disk. This folder is where that
   * happens, and it is what makes a second read of the same revision free.
   *
   * <p>Empty means a folder under the system temporary directory, which is the right answer for the
   * container a deployment runs in and the wrong one for a long lived server.
   */
  @GuiWidgetElement(
      id = "30000-cache-folder",
      order = "1000",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::GitConnection.CacheFolder.Label",
      toolTip = "i18n::GitConnection.CacheFolder.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_ADVANCED,
      groupOrder = "030")
  @HopMetadataProperty
  private String cacheFolder;

  /**
   * Fetch the revision again on every first use, rather than reusing the checkout of an earlier
   * run.
   *
   * <p>Off, the default, is what a deployment wants: the revision is fixed, so a checkout which is
   * already there is the same one the fetch would produce. On is what a developer wants while
   * watching a branch move.
   */
  @GuiWidgetElement(
      id = "30100-always-fetch",
      order = "1100",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::GitConnection.AlwaysFetch.Label",
      toolTip = "i18n::GitConnection.AlwaysFetch.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_ADVANCED,
      groupOrder = "030")
  @HopMetadataProperty
  private boolean alwaysFetch;

  /**
   * How long a checkout may be reused, in minutes, before it is fetched again.
   *
   * <p>Only applies when {@link #alwaysFetch} is off: an empty value, the default, means a checkout
   * never goes stale on its own.
   */
  @GuiWidgetElement(
      id = "30200-cache-minutes",
      order = "1200",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.CacheMinutes.Label",
      toolTip = "i18n::GitConnection.CacheMinutes.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_ADVANCED,
      groupOrder = "030")
  @HopMetadataProperty
  private String cacheMinutes;

  /**
   * Refuse writes. A write only changes the working copy: nothing is committed and nothing is
   * pushed.
   */
  @GuiWidgetElement(
      id = "30300-read-only",
      order = "1300",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::GitConnection.ReadOnly.Label",
      toolTip = "i18n::GitConnection.ReadOnly.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_ADVANCED,
      groupOrder = "030")
  @HopMetadataProperty
  private boolean readOnly = true;

  /** How long to wait for the remote to answer, in seconds. Empty for the JGit default. */
  @GuiWidgetElement(
      id = "30400-timeout",
      order = "1400",
      parentId = GitConnectionEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GitConnection.Timeout.Label",
      toolTip = "i18n::GitConnection.Timeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_ADVANCED,
      groupOrder = "030")
  @HopMetadataProperty
  private String timeoutSeconds;

  public GitConnection() {
    authType = GitAuthType.NONE;
    readOnly = true;
  }

  /** Never null: a connection saved before the auth types existed means no authentication. */
  public GitAuthType getAuthType() {
    return authType == null ? GitAuthType.NONE : authType;
  }
}
