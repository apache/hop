package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.util.ReflectionUtil;

@ExtensionPoint(
  id = "GitRepositoryCreateExtensionPoint",
  extensionPointId = "GitRepositoryCreate",
  description = "Sets the name and base folder of a new git repository to the base of the environment"
)
public class GitRepositoryCreateExtensionPoint implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object environment ) throws HopException {
    ReflectionUtil.setFieldValue( environment, "name", String.class, "git");
    ReflectionUtil.setFieldValue( environment, "directory", String.class, "${ENVIRONMENT_HOME}");
  }
}
