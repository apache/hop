package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;

import java.util.Map;

@ExtensionPoint(
  id = "ProjectsVariablesControlSpaceSortOrderPrefix",
  extensionPointId = "HopGuiGetControlSpaceSortOrderPrefix",
  description = "Set a prefix sort order for the projects variables, push to front of the list"
)
public class ProjectsVariablesControlSpaceSortOrderPrefix
    implements IExtensionPoint<Map<String, String>> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, Map<String, String> prefixMap) throws HopException {

    prefixMap.put(ProjectsUtil.VARIABLE_PROJECT_HOME, "310_");
    prefixMap.put( Defaults.VARIABLE_HOP_PROJECT_NAME, "450_");
    prefixMap.put( Defaults.VARIABLE_HOP_ENVIRONMENT_NAME, "450_");
    prefixMap.put( ProjectsUtil.VARIABLE_HOP_DATASETS_FOLDER, "450_");
    prefixMap.put( ProjectsUtil.VARIABLE_HOP_UNIT_TESTS_FOLDER, "460_");

  }
}
