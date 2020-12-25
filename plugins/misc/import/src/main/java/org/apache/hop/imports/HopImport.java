package org.apache.hop.imports;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.ui.hopgui.HopGui;

import javax.xml.transform.dom.DOMSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class HopImport implements IHopImport{

    private static IHopMetadataProvider metadataProvider;
    private static String inputFolderName, outputFolderName;

    public TreeMap<String, String> connectionFileList;
    public List<DatabaseMeta> connectionsList;

    public PluginRegistry registry;

    public File inputFolder, outputFolder;
    public ILogChannel log;

    public HashMap<String, DOMSource> migratedFilesMap;

    private ProjectsConfig config;

    public HopImport(){
        HopGui hopGui = HopGui.getInstance();
        metadataProvider = hopGui.getMetadataProvider();
        registry = PluginRegistry.getInstance();

        log = hopGui.getLog();

        connectionsList = new ArrayList<DatabaseMeta>();
        connectionFileList = new TreeMap<String, String>();
        migratedFilesMap = new HashMap<String, DOMSource>();
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
    }

    public void addDatabaseMeta(String filename, DatabaseMeta databaseMeta) {
        // build a list of all jobs, transformations with their connections
        connectionFileList.put(filename, databaseMeta.getName());
        // only add new connection name to the list
        if(connectionsList.stream().filter(dbMeta -> dbMeta.getName().equals(databaseMeta.getName())).collect(Collectors.toList()).size() == 0){
            connectionsList.add(databaseMeta);
        }
    }
}
