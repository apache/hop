package org.apache.hop.projects.gui;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

@GuiPlugin(description = "")
public class ProjectsEnvironmentsDialog /*extends Dialog implements KeyListener*/ {

    private SashForm sashForm, rightSash;
    private Tree projectsTree, environmentsTree;
    private IVariables variables;
    private List<String> projectNames;
    private PropsUi props;
    private Composite envComp, projectsComp;
    private ToolBar projectToolbar, envToolbar;
    private GuiToolbarWidgets projectToolbarWidgets, envToolbarWidgets;
    public static final String CONFIG_PERSPECTIVE_PROJENV_TAB_ID = "config-perspective-project-tab-id";
    private static final String ID_PROJECT_TOOLBAR = "toolbar-project";
    private static final String ID_ENV_TOOLBAR = "toolbar-env";
    public static final String ID_TOOLBAR_PROJECT_EDIT = "toolbar-edit-project";
    public static final String ID_TOOLBAR_PROJECT_ADD = "toolbar-add-project";
    public static final String ID_TOOLBAR_PROJECT_DELETE = "toolbar-delete-project";
    public static final String ID_TOOLBAR_ENVIRONMENT_EDIT = "toolbar-edit-env";
    public static final String ID_TOOLBAR_ENVIRONMENT_ADD = "toolbar-add-env";
    public static final String ID_TOOLBAR_ENVIRONMENT_DELETE = "toolbar-delete-env";


    public void editProject(){}

    @GuiTab(
            parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
            id = CONFIG_PERSPECTIVE_PROJENV_TAB_ID,
            description = "gui-tab-config-perspective-description",
            tabPosition = 25
    )
    public void addProjectsEnvironmentsTab(Composite composite){

        CTabFolder cTabFolder = (CTabFolder) composite;

        CTabItem projectEnvItem = new CTabItem(cTabFolder, SWT.NONE);
        projectEnvItem.setText("Projects Environments");

        props = PropsUi.getInstance();
        int margin = props.getMargin();
        int middle = Const.MIDDLE_PCT;

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        cTabFolder.setLayout(formLayout);

        sashForm = new SashForm(cTabFolder, SWT.HORIZONTAL);
        projectEnvItem.setControl(sashForm);

        Composite projectBrowser = new Composite(sashForm, SWT.NONE);
        props.setLook(projectBrowser);
        FormLayout projLayout = new FormLayout();
        projLayout.marginWidth = Const.FORM_MARGIN;
        projLayout.marginHeight = Const.FORM_MARGIN;
        projectBrowser.setLayout(projLayout);

        projectToolbar = new ToolBar(projectBrowser, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
        FormData fdProjToolbar = new FormData();
        fdProjToolbar.left = new FormAttachment(0, 0);
        fdProjToolbar.top = new FormAttachment(0, 0);
        fdProjToolbar.right = new FormAttachment(100, 0);
        projectToolbar.setLayoutData(fdProjToolbar);
        props.setLook(projectToolbar, Props.WIDGET_STYLE_TOOLBAR);

        projectToolbarWidgets = new GuiToolbarWidgets();
        projectToolbarWidgets.registerGuiPluginObject(this);
        projectToolbarWidgets.createToolbarWidgets(projectToolbar, ID_PROJECT_TOOLBAR);
        projectToolbar.pack();

        getProjects();

        projectsTree = new Tree(projectBrowser, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
        props.setLook(projectsTree);

        FormData fdTProjectBrowser = new FormData();
        fdTProjectBrowser.left = new FormAttachment(0,0);
        fdTProjectBrowser.top = new FormAttachment(projectToolbar, 0);
        fdTProjectBrowser.right = new FormAttachment(100, 0);
        fdTProjectBrowser.bottom = new FormAttachment(100, -margin);
        projectsTree.setLayoutData(fdTProjectBrowser);
        for(int i=0; i < projectNames.size(); i++){
            TreeItem item = new TreeItem(projectsTree, SWT.NONE);
            item.setText(projectNames.get(i));
        }

        // right side panel
        rightSash = new SashForm(sashForm, SWT.VERTICAL);
        props.setLook(rightSash);
        FormLayout rightSashLayout = new FormLayout();
        rightSashLayout.marginWidth = Const.FORM_MARGIN;
        rightSashLayout.marginHeight = Const.FORM_MARGIN;
        rightSash.setLayout(rightSashLayout);
        FormData fdRightSash = new FormData();
        fdRightSash.left = new FormAttachment(0,0);
        fdRightSash.right = new FormAttachment(100, 0);
        fdRightSash.top = new FormAttachment(0,0);
        fdRightSash.bottom = new FormAttachment(100, 0);
        rightSash.setLayoutData(fdRightSash);

        Composite envBrowser = new Composite(rightSash, SWT.NONE);
        props.setLook(envBrowser);
        FormLayout envLayout = new FormLayout();
        envLayout.marginWidth = Const.MARGIN;
        envLayout.marginHeight = Const.MARGIN;
        FormData fdEnvBrowser = new FormData();
        fdEnvBrowser.left = new FormAttachment(0, 0);
        fdEnvBrowser.right = new FormAttachment(100,0);
        fdEnvBrowser.top = new FormAttachment(0,0);
        fdEnvBrowser.bottom = new FormAttachment(0,0);
        envBrowser.setLayoutData(fdEnvBrowser);
        envBrowser.setLayout(envLayout);

        envToolbar = new ToolBar(envBrowser, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
        FormData fdEnvToolbar = new FormData();
        fdEnvToolbar.left = new FormAttachment(0, 0);
        fdEnvToolbar.top = new FormAttachment(0, 0);
        fdEnvToolbar.right = new FormAttachment(100, 0);
        envToolbar.setLayoutData(fdEnvToolbar);
        props.setLook(envToolbar, Props.WIDGET_STYLE_TOOLBAR);
        envToolbarWidgets = new GuiToolbarWidgets();
        envToolbarWidgets.registerGuiPluginObject(this);
        envToolbarWidgets.createToolbarWidgets(envToolbar, ID_ENV_TOOLBAR);
        envToolbar.pack();

        environmentsTree = new Tree(envBrowser, SWT.SINGLE);
        FormData fdTEnvBrowser = new FormData();
        fdTEnvBrowser.left = new FormAttachment(0,0);
        fdTEnvBrowser.top = new FormAttachment(envToolbar, margin);
        fdTEnvBrowser.right = new FormAttachment(100, 0);
        fdTEnvBrowser.bottom = new FormAttachment(100, -margin);
        environmentsTree.setLayoutData(fdTEnvBrowser);

        Composite projEnvDetails = new Composite(rightSash, SWT.NONE);
        props.setLook(projEnvDetails);
        FormLayout projEnvLayout = new FormLayout();
        projEnvLayout.marginWidth = Const.MARGIN;
        projEnvLayout.marginHeight = Const.MARGIN;
        FormData fdProjEnvDetails = new FormData();
        fdProjEnvDetails.top = new FormAttachment(0,0);
        fdProjEnvDetails.bottom = new FormAttachment(100, 0);
        fdProjEnvDetails.left = new FormAttachment(0,0);
        fdProjEnvDetails.right = new FormAttachment(100, 0);
        projEnvDetails.setLayoutData(fdProjEnvDetails);
        projEnvDetails.setLayout(projEnvLayout);

        // Project details
        projectsComp = new Composite(projEnvDetails, SWT.NONE);
        FormData fdProjDetails = new FormData();
        fdProjDetails.left = new FormAttachment(0,0);
        fdProjDetails.right = new FormAttachment(100, 0);
        fdProjDetails.top = new FormAttachment(0, -margin);
        fdProjDetails.bottom = new FormAttachment(100, -margin);
        projectsComp.setLayoutData(fdProjDetails);
        projectsComp.setLayout(projLayout);
//        projectsComp.setVisible(false);

        Label projectWlName = new Label(projectsComp, SWT.RIGHT);
        props.setLook(projectWlName);
        projectWlName.setText("Project Details");
        FormData projectFdlName = new FormData();
        projectFdlName.left = new FormAttachment(0,0);
        projectFdlName.right = new FormAttachment(middle, 0);
        projectFdlName.top = new FormAttachment(0, margin);
        projectWlName.setLayoutData(projectFdlName);

        // Environment details
        envComp = new Composite(projEnvDetails, SWT.NONE);
        FormData fdEnvDetails = new FormData();
        fdEnvDetails.left = new FormAttachment(0,0);
        fdEnvDetails.right = new FormAttachment(100, 0);
        fdEnvDetails.top = new FormAttachment(0, -margin);
        fdEnvDetails.bottom = new FormAttachment(100, -margin);
        envComp.setLayoutData(fdEnvDetails);

        Label envWlName = new Label(envComp, SWT.RIGHT);
        props.setLook(envWlName);
        envWlName.setText("Environment Details");
        FormData envFdlName = new FormData();
        envFdlName.left = new FormAttachment(0,0);
        envFdlName.right = new FormAttachment(middle, 0);
        envFdlName.top = new FormAttachment(0,margin);
        envWlName.setLayoutData(envFdlName);
        envComp.setLayout(envLayout);

        FormData fdSash = new FormData();
        fdSash.top = new FormAttachment(0, 0);
        fdSash.bottom = new FormAttachment(100, 0);
        fdSash.left = new FormAttachment(0,0);
        fdSash.right = new FormAttachment(100, 0);
        sashForm.setLayoutData(fdSash);

        sashForm.setWeights(25, 75);
        rightSash.setWeights(40, 60);

        projectsTree.addSelectionListener(
                new SelectionAdapter() {
                    @Override
                    public void widgetSelected(SelectionEvent e) {
                        projectsComp.setVisible(true);
                        envComp.setVisible(false);
                        getEnvironments(projectsTree.getSelection()[0].getText());
                    }
                }
        );

        environmentsTree.addSelectionListener(
                new SelectionAdapter() {
                    @Override
                    public void widgetSelected(SelectionEvent e) {
                        projectsComp.setVisible(false);
                        envComp.setVisible(true);
                    }
                }
        );
    }

    @GuiToolbarElement(
            root = ID_PROJECT_TOOLBAR,
            id = ID_TOOLBAR_PROJECT_EDIT,
            toolTip = "i18n::HopGui.Toolbar.Project.Edit.Tooltip",
            image = "project-edit.svg")
    public void editSelectedProject() {
        editProject();
    }

    @GuiToolbarElement(
            root = ID_PROJECT_TOOLBAR,
            id = ID_TOOLBAR_PROJECT_ADD,
            toolTip = "i18n::HopGui.Toolbar.Project.Add.Tooltip",
            image = "project-add.svg")
    public void addNewProject() {}

    @GuiToolbarElement(
            root = ID_PROJECT_TOOLBAR,
            id = ID_TOOLBAR_PROJECT_DELETE,
            toolTip = "i18n::HopGui.Toolbar.Project.Delete.Tooltip",
            image = "project-delete.svg",
            separator = true)
    public void deleteSelectedProject() {}

    @GuiToolbarElement(
            root = ID_ENV_TOOLBAR,
            id = ID_TOOLBAR_ENVIRONMENT_EDIT,
            toolTip = "i18n::HopGui.Toolbar.Environment.Edit.Tooltip",
            image = "environment-edit.svg")
    public void editSelectedEnvironment() {}

    @GuiToolbarElement(
            root = ID_ENV_TOOLBAR,
            id = ID_TOOLBAR_ENVIRONMENT_ADD,
            toolTip = "i18n::HopGui.Toolbar.Environment.Add.Tooltip",
            image = "environment-add.svg")
    public void addNewEnvironment() {}

    @GuiToolbarElement(
            root = ID_ENV_TOOLBAR,
            id = ID_TOOLBAR_ENVIRONMENT_DELETE,
            toolTip = "i18n::HopGui.Toolbar.Environment.Delete.Tooltip",
            image = "environment-delete.svg",
            separator = true)
    public void deleteSelectedEnvironment() {}

    public String open(){
        return "";
    }

    private void ok(){};
    private void cancel(){};

    private List<String> getProjects(){
        projectNames = ProjectsConfigSingleton.getConfig().listProjectConfigNames();
        return projectNames;
    };

    private List<String> getEnvironments(String projectName){
        TreeItem[] envItems = environmentsTree.getItems();
        for(TreeItem envItem : envItems){
            envItem.dispose();
        }
        List<String> projEnvNames = new ArrayList<>();
        ProjectsConfig config = ProjectsConfigSingleton.getConfig();
        List<String> envNames = ProjectsConfigSingleton.getConfig().listEnvironmentNames();
        for(String envName : envNames){
            LifecycleEnvironment environment = config.findEnvironment(envName);
            if(environment.getProjectName().equals(projectName)){
                TreeItem item = new TreeItem(environmentsTree, SWT.NONE);
                item.setText(environment.getName());
                projEnvNames.add(envName);
            }
        }
        return projEnvNames;
    }
}
