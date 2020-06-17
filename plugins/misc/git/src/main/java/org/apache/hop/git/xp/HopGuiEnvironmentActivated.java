package org.apache.hop.git.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.git.HopGitPerspective;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;

/**
 * Reload the available git repositories when an environment is changed...
 */
@ExtensionPoint(
  id = "HopGuiEnvironmentActivated",
  extensionPointId = "EnvironmentActivated",
  description = "Reload the available git repositories when an environment is changed")
public class HopGuiEnvironmentActivated implements IExtensionPoint<String> {

  @Override public void callExtensionPoint( ILogChannel log, String environmentName ) throws HopException {
    HopGitPerspective gitPerspective = HopGitPerspective.getInstance();
    gitPerspective.refreshGitRepositoriesList();
    gitPerspective.clearRepository();

    // See if there's a history.  Open the last used git repository
    //
    String lastUsedRepositoryName = AuditManagerGuiUtil.getLastUsedValue( HopGitPerspective.AUDIT_TYPE );
    if ( StringUtils.isEmpty(lastUsedRepositoryName)) {
      return;
    }

    gitPerspective.loadRepository( lastUsedRepositoryName );
  }

}
