package org.apache.hop.git.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.git.HopGitPerspective;
import org.apache.hop.git.model.repository.GitRepository;

/**
 * Used for create/update/delete of Git Repository metadata objects
 */
public class HopGuiGitRepositoryChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object o ) throws HopException {
    if (!(o instanceof GitRepository )) {
      return;
    }

    // We want to reload the combo box in the Git perspective GUI plugin
    //
    HopGitPerspective.refreshGitRepositoriesList();
  }
}
