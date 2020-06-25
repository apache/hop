package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.apache.hop.projects.util.ProjectsUtil;

@ExtensionPoint(
  id = "GitRepositoryCreateExtensionPoint",
  extensionPointId = "GitRepositoryCreate",
  description = "Sets the name and base folder of a new git repository to the base of the project"
)
public class GitRepositoryCreateExtensionPoint implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object project ) throws HopException {
    ReflectionUtil.setFieldValue( project, "name", String.class, "git");
    ReflectionUtil.setFieldValue( project, "directory", String.class, "${"+ ProjectsUtil.VARIABLE_PROJECT_HOME+"}");
  }
}
