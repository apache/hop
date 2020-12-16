package org.apache.hop.projects.xp;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@ExtensionPoint(
        id = "HopProjectInformation",
        description = "Provides information about the Hop project environment",
        extensionPointId = "HopProjectInformation"
)
public class HopProjectInformation implements IExtensionPoint<HashMap<String,Object>> {

    @Override
    public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, HashMap<String,Object> importProjectMap) throws HopException{

        HopGui hopGui = HopGui.getInstance();
        ProjectsConfig config = ProjectsConfigSingleton.getConfig();

        String importProject = (String)importProjectMap.get("importToProject");
        String importPath = (String)importProjectMap.get("importToFolder");

        // open existing project
        if(!StringUtil.isEmpty((String)importProjectMap.get(importProject))){
            ProjectConfig projectConfig = config.findProjectConfig(importProject);
            LifecycleEnvironment environment = null;
            List<LifecycleEnvironment> environments = config.findEnvironmentsOfProject(importProject );
            if ( !environments.isEmpty() ) {
                environment = environments.get( 0 );
            }

            try {
                Project project = projectConfig.loadProject( hopGui.getVariables() );
                ProjectsUtil.enableProject(iLogChannel, importProject, project, variables, null, null, hopGui);
                hopGui.setVariables(variables);
            } catch ( Exception e ) {
                new ErrorDialog( hopGui.getShell(), "Error", "Error changing project to '" + importProject, e );
            }
        }

        if(!StringUtil.isEmpty(importPath)){
            ProjectConfig projectConfig = new ProjectConfig("Hop Import Project", importPath, ProjectConfig.DEFAULT_PROJECT_CONFIG_FILENAME);
            Project project = new Project();

            project.modifyVariables(variables, projectConfig, Collections.emptyList(), null);
            project.setConfigFilename(importPath + System.getProperty("file.separator") + "project-config.json");
            config.addProjectConfig(projectConfig);
            HopConfig.getInstance().saveToFile();
            project.saveToFile();
            ProjectsUtil.enableProject(hopGui.getLog(), "Hop Import Project", project, variables, null, null, hopGui);
        }
    }
}
