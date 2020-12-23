package org.apache.hop.imports;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.HopGui;

import java.io.*;
import java.util.Properties;

public class HopImport implements IHopImport{

    private static IHopMetadataProvider metadataProvider;
    private static String inputFolderName, outputFolderName;
    public PluginRegistry registry;
    public IHopMetadataSerializer<DatabaseMeta> databaseSerializer;

    public File inputFolder, outputFolder;
    public LogChannel log;

    private ProjectsConfig config;

    public HopImport(){
        try {
            HopGui hopGui = HopGui.getInstance();
            metadataProvider = hopGui.getMetadataProvider();
            registry = PluginRegistry.getInstance();

            databaseSerializer = metadataProvider.getSerializer(DatabaseMeta.class);
            log = new LogChannel("Hop Import");

        } catch (HopException e) {
            e.printStackTrace();
        }
    }

    public void importHopFolder(){
    }

    public void importHopFile(File importFile){
    }

    public HopImport(String inputFolderName, String outputFolderName){
        this();
        setInputFolder(inputFolderName);
        setOutputFolder(outputFolderName);
    }

    public IVariables importVars(String varFile, HopVarImport varType, IVariables variables) {
        try{
            switch(varType){
                case PROPERTIES:
                    return importVarsFromProperties(varFile, variables);
                case XML:
                    break;
                default:
                    break;
            }
        }catch(IOException e){
            e.printStackTrace();
        }
        return null;
    }

    public void importConnections(String dbConnPath, HopDbConnImport connType){
        switch(connType){
            case PROPERTIES:
                importPropertiesDbConn(dbConnPath);
                break;
            case XML:
                importXmlDbConn(dbConnPath);
                break;
            default:
                break;
        }
    }

    private IVariables importVarsFromProperties(String varFilePath, IVariables variables) throws IOException {
        Properties properties = new Properties();
        File varFile = new File(varFilePath);
        InputStream inputStream = new FileInputStream(varFile);
        properties.load(inputStream);

        Variables projectVars = new Variables();

        properties.forEach((k,v) -> {
            projectVars.setVariable((String)k, (String)v);
            log.logBasic("Saved variable " + (String)k + ": " + (String)v);
        });

        return projectVars;
    }

    public void importXmlDbConn(String dbConnPath){
    }

    public void importPropertiesDbConn(String dbConnPath){

    }

    public String getInputFolder() {
        return inputFolderName;
    }

    public void setInputFolder(String inputFolderName) {
        this.inputFolderName = inputFolderName;
        inputFolder = new File(inputFolderName);
        if(!inputFolder.exists() || !inputFolder.isDirectory()){
            log.logBasic("input folder '" + inputFolderName + "' doesn't exist or is not a folder.");
        }
    }

    public String getOutputFolder() {
        return outputFolderName;
    }

    public void setOutputFolder(String outputFolderName) {
        this.outputFolderName = outputFolderName;
        outputFolder = new File(outputFolderName);

        if(!outputFolder.exists() || !outputFolder.isDirectory()){
            log.logBasic("output folder '" + outputFolderName + "' doesn't exist or is not a folder.");
            outputFolder.mkdir();
        }

/*
        if(outputFolder.listFiles().length > 0){
            try{
                Files.walk(outputFolder.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }catch(IOException e){
                log.logError(outputFolderName + " could not be cleared");
                e.printStackTrace();
            }
        }
*/
    }

/*
    public void openProject(String projectName){
        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        if(projectConfig == null){
            projectConfig = new ProjectConfig();

        }

    }
*/

/*
    public Object getProjectConfig(){
        try{
            String guiPluginId = "org.apache.hop.projects.gui.ProjectsGuiPlugin";
            String singletonClassName = "org.apache.hop.projects.config.ProjectsConfigSingleton";
            String methodName = "listProjectConfigNames";

            PluginRegistry registry = PluginRegistry.getInstance();
            IPlugin guiPlugin = registry.getPlugin( GuiPluginType.class, guiPluginId );
            ClassLoader classLoader = registry.getClassLoader( guiPlugin );

            Class<?> singletonClass = classLoader.loadClass( singletonClassName );
            Method getInstanceMethod = singletonClass.getDeclaredMethod("getConfig", new Class[] { });
            Object singleton = getInstanceMethod.invoke( null, new Object[] {} );

            Method method = classLoader.loadClass("org.apache.hop.projects.config.ProjectsConfig").getMethod("findProjectConfig", String.class);

            return method.invoke(singleton, "hop-neo4j");

                Method method = classLoader.loadClass("org.apache.hop.projects.config.ProjectsConfig").getMethod(methodName);
                List<String> values = (List<String>) method.invoke( singleton, new Object[] {} );

                values.forEach(value -> {
                    System.out.println("############################: " + value);
                });
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch(HopPluginException e){
            e.printStackTrace();
        }
        return null;
    }
*/

}
