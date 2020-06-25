package org.apache.hop.projects.search;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.projects.project.ProjectConfig;

import java.util.Iterator;

public class ProjectsSearchablesLocation implements ISearchablesLocation {

  private ProjectConfig projectConfig;

  public ProjectsSearchablesLocation( ProjectConfig projectConfig ) {
    this.projectConfig = projectConfig;
  }

  @Override public String getLocationDescription() {
    return "Project "+projectConfig.getProjectName();
  }

  @Override public Iterator<ISearchable> getSearchables() throws HopException {
    return new ProjectSearchablesIterator( projectConfig );
  }

}
