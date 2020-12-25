package org.apache.hop.projects.xp;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.HopGui;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

@ExtensionPoint(
        id = "HopImportConnections",
        description = "Import relational database connections into a Hop project",
        extensionPointId = "HopImportConnections"
)
public class HopImportDbConnections implements IExtensionPoint<Object[]> {

    @Override
    public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, Object[] connectionObject) throws HopException {
        String projectName = (String)connectionObject[0];
        List<DatabaseMeta> connectionList = (List<DatabaseMeta>)connectionObject[1];
        TreeMap<String, String> connectionFileMap = (TreeMap<String, String>)connectionObject[2];

        HopGui hopGui = HopGui.getInstance();
        ILogChannel log = hopGui.getLog();
        ProjectsConfig config = ProjectsConfigSingleton.getConfig();

        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        Project project = projectConfig.loadProject( hopGui.getVariables() );
        projectConfig.getProjectHome();

        IHopMetadataProvider metadataProvider = HopGui.getInstance().getMetadataProvider();
        IHopMetadataSerializer<DatabaseMeta> databaseSerializer = metadataProvider.getSerializer(DatabaseMeta.class);

        Iterator<DatabaseMeta> connectionIterator = connectionList.iterator();
        while(connectionIterator.hasNext()){
            DatabaseMeta databaseMeta = connectionIterator.next();
            try {
                if(databaseSerializer.exists(databaseMeta.getName())){
                    log.logBasic("Skipped: a connection with name '" + databaseMeta.getName() + "' already exists.");
                }else{
                    databaseSerializer.save(databaseMeta);
                }
            } catch (HopException e) {
                e.printStackTrace();
            }
        }

        String eol = System.getProperty("line.separator");

        try (Writer writer = new FileWriter(projectConfig.getProjectHome() + System.getProperty("file.separator") + "connections.csv")) {
            for (Map.Entry<String, String> entry : connectionFileMap.entrySet()) {
                writer.append(entry.getKey())
                        .append(',')
                        .append(entry.getValue())
                        .append(eol);
            }
        } catch (IOException e) {
            log.logError("Error writing connections file to project : ");
            e.printStackTrace();
        }
    }
}
