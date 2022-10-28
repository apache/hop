/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.gui;

import java.util.*;
import java.util.List;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

@GuiPlugin(description = "")
public class ProjectsEnvironmentsDialog implements TabClosable {

    private SashForm sashForm;
    private Tree projectsTree;
    private PropsUi props;
//    private Composite envComp, projectsComp;
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
    private HopGui hopGui;
    private ProjectsConfig projectsConfig;
    private List<String> envNames;
    private int iconSize;
    private Image folderImage, fileImage;

    public ProjectsEnvironmentsDialog(){

        hopGui = HopGui.getInstance();
        iconSize = (int) (PropsUi.getInstance().getZoomFactor() * 16);

        try{
            // folder icon for projects
            SvgCacheEntry folderSvgCacheEntry = SvgCache.loadSvg(new SvgFile("ui/images/folder.svg", hopGui.getClass().getClassLoader().getParent()));
            SwtUniversalImageSvg folderImageSvg = new SwtUniversalImageSvg(new SvgImage(folderSvgCacheEntry.getSvgDocument()));
            folderImage = folderImageSvg.getAsBitmapForSize(hopGui.getDisplay(), iconSize, iconSize);

            // file icon for environments
            SvgCacheEntry fileSvgCacheEntry = SvgCache.loadSvg(new SvgFile("ui/images/file.svg", hopGui.getClass().getClassLoader()));
            SwtUniversalImageSvg fileImageSvg = new SwtUniversalImageSvg(new SvgImage(fileSvgCacheEntry.getSvgDocument()));
            fileImage = fileImageSvg.getAsBitmapForSize(hopGui.getDisplay(), iconSize, iconSize);
        }catch(HopException e){
            hopGui.getLog().logError("Error building projects and environments tree", e);
        }

        projectsConfig = ProjectsConfigSingleton.getConfig();
        envNames = projectsConfig.listEnvironmentNames();
    }

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

//        getProjects();

        projectsTree = new Tree(projectBrowser, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
        props.setLook(projectsTree);

        FormData fdTProjectBrowser = new FormData();
        fdTProjectBrowser.left = new FormAttachment(0,0);
        fdTProjectBrowser.top = new FormAttachment(projectToolbar, 0);
        fdTProjectBrowser.right = new FormAttachment(100, 0);
        fdTProjectBrowser.bottom = new FormAttachment(100, -margin);
        projectsTree.setLayoutData(fdTProjectBrowser);

        buildProjEnvTree();

        // right hand side composite
        CTabFolder projEnvTabs = new CTabFolder(sashForm, SWT.NONE);
        FormLayout projEnvTabsLayout = new FormLayout();
        projEnvTabsLayout.marginWidth= Const.FORM_MARGIN;
        projEnvTabsLayout.marginHeight = Const.FORM_MARGIN;
        projEnvTabs.setLayout(projEnvTabsLayout);
        FormData fdProjEnvTabs = new FormData();
        fdProjEnvTabs.left = new FormAttachment(0,0);
        fdProjEnvTabs.top = new FormAttachment(0,0);
        fdProjEnvTabs.right = new FormAttachment(100,0);
        fdProjEnvTabs.bottom = new FormAttachment(100,0);
        projEnvTabs.setLayoutData(fdProjEnvTabs);

/*
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
*/

        FormData fdSash = new FormData();
        fdSash.top = new FormAttachment(0, 0);
        fdSash.bottom = new FormAttachment(100, 0);
        fdSash.left = new FormAttachment(0,0);
        fdSash.right = new FormAttachment(100, 0);
        sashForm.setLayoutData(fdSash);

        sashForm.setWeights(25, 75);

        projectsTree.addSelectionListener(
                new SelectionAdapter() {
                    @Override
                    public void widgetSelected(SelectionEvent e) {
//                        projectsComp.setVisible(true);
//                        envComp.setVisible(false);
                    }
                }
        );

        projectsTree.addMenuDetectListener(
                event -> {
                    if(projectsTree.getSelectionCount() < 1){
                        return;
                    }
                    TreeItem treeItem = projectsTree.getSelection()[0];
                    Image itemImage = treeItem.getImage();
                    if(treeItem != null){
                        Menu menu = new Menu(projectsTree);

                        MenuItem editMenuItem = new MenuItem(menu, SWT.POP_UP);
                        editMenuItem.setText("Edit");
                        editMenuItem.addListener(SWT.Selection, e -> {
                            if (itemImage.equals(folderImage)) {
                                editSelectedProject();
                            }else if(itemImage.equals(fileImage)){
                                editSelectedEnvironment();
                            }

                        });

                        MenuItem deleteMenuItem = new MenuItem(menu, SWT.POP_UP);
                        deleteMenuItem.setText("Delete");
                        deleteMenuItem.addListener(SWT.Selection, e -> {});

                        projectsTree.setMenu(menu);
                        menu.setVisible(true);

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


    private void buildProjEnvTree(){
        // build the list of parent and child project names
        // only one level deep for now
        List<String> projectConfigNames = projectsConfig.listProjectConfigNames();
        HopGui hopGui = HopGui.getInstance();
        MultiValuedMap<String, String> parentChildMap = new ArrayListValuedHashMap<>();
        for(String projectConfigName : projectConfigNames){
            ProjectConfig projectConfig = projectsConfig.findProjectConfig(projectConfigName);
            try{
                Project project = projectConfig.loadProject(hopGui.getVariables());
                String parentProject = project.getParentProjectName();
                if(!StringUtils.isEmpty(parentProject)){
                    parentChildMap.put(project.getParentProjectName(), projectConfigName);
                }else{
                    parentChildMap.put(projectConfigName, "");
                }
            }catch(HopException e){
                hopGui.getLog().logError("error processing " + projectConfigName);
            }
        }
        List<String> projectsList = new ArrayList<>(parentChildMap.keySet());
        Collections.sort(projectsList);

        for(String projectName : projectsList){
            TreeItem projectItem = new TreeItem(projectsTree, SWT.NONE);
            projectItem.setText(projectName);
            projectItem.setImage(folderImage);
            List<String> childProjectNames = (List<String>) parentChildMap.get(projectName);
            if(childProjectNames.size() > 1){
                for(String childProjectName : childProjectNames){
                    if(!StringUtils.isEmpty(childProjectName )){
                        TreeItem childProjectItem = new TreeItem(projectItem, SWT.NONE);
                        childProjectItem.setText(childProjectName);
                        childProjectItem.setImage(folderImage);
                        addEnvTreeItems(childProjectItem, childProjectName);
                    }
                }
            }
            addEnvTreeItems(projectItem, projectName);
        }
    }

    private void addEnvTreeItems(TreeItem projectItem, String projectName){
        if(!envNames.isEmpty()){
            for(String envName : envNames){
                LifecycleEnvironment environment = projectsConfig.findEnvironment(envName);
                if(environment.getProjectName().equals(projectName)){
                    TreeItem envItem = new TreeItem(projectItem, SWT.NONE);
                    envItem.setText(envName);
                    envItem.setImage(fileImage);
                }
            }
        }
    }

    private void editProject(){

    }


    @Override
    public void closeTab(CTabFolderEvent event, CTabItem tabItem) {

    }

    @Override
    public List<CTabItem> getTabsToRight(CTabItem selectedTabItem) {
        return TabClosable.super.getTabsToRight(selectedTabItem);
    }

    @Override
    public List<CTabItem> getTabsToLeft(CTabItem selectedTabItem) {
        return TabClosable.super.getTabsToLeft(selectedTabItem);
    }

    @Override
    public List<CTabItem> getOtherTabs(CTabItem selectedTabItem) {
        return TabClosable.super.getOtherTabs(selectedTabItem);
    }

    @Override
    public CTabFolder getTabFolder() {
        return null;
    }
}
