package org.apache.hop.projects.search;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;

import java.util.List;

@ExtensionPoint(
  id = "AddProjectsSearchablesLocationExtensionPoint",
  description = "Adds a search location to the search perspective",
  extensionPointId = "HopGuiGetSearchablesLocations"
)
public class AddProjectsSearchablesLocationExtensionPoint implements IExtensionPoint<List<ISearchablesLocation>> {
  @Override public void callExtensionPoint( ILogChannel log, List<ISearchablesLocation> searchablesLocations ) throws HopException {

    // The location to add is the currently active project and the files in the home folder
    //
    String projectName = HopNamespace.getNamespace();
    if (projectName==null) {
      return;
    }
    ProjectConfig projectConfig = ProjectsConfigSingleton.getConfig().findProjectConfig( projectName );
    if (projectConfig==null) {
      return;
    }
    try {
      ProjectsSearchablesLocation projectsSearchablesLocation = new ProjectsSearchablesLocation( projectConfig );
      searchablesLocations.add( projectsSearchablesLocation );
    } catch(Exception e) {
      log.logError( "Error getting project searchables", e );
    }
  }
}
