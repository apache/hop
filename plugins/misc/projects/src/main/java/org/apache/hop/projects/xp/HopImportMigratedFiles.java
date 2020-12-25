package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.HopGui;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;

@ExtensionPoint(
        id = "HopImportMigratedFiles",
        description = "Imports variables into a Hop project",
        extensionPointId = "HopImportMigratedFiles"
)
public class HopImportMigratedFiles implements IExtensionPoint<Object[]> {

    @Override
    public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, Object[] migrationObject) throws HopException {
        String projectName = (String)migrationObject[0];
        HashMap<String, DOMSource> filesMap = (HashMap<String, DOMSource>)migrationObject[1];

        HopGui hopGui = HopGui.getInstance();
        ProjectsConfig config = ProjectsConfigSingleton.getConfig();

        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        Project project = projectConfig.loadProject( hopGui.getVariables() );
        projectConfig.getProjectHome();

        try {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");

            /**
             * TODO: check if no import path is provided (import into selected project).
             */
            Iterator<String> filesIterator = filesMap.keySet().iterator();
            while(filesIterator.hasNext()) {
                String outFilename = filesIterator.next();
                DOMSource domSource = filesMap.get(outFilename);

                if(outFilename.indexOf(System.getProperty("user.dir")) > -1){
                    outFilename = outFilename.replaceAll(System.getProperty("user.dir"), "");
                    outFilename = projectConfig.getProjectHome() + outFilename;
                }

                File outFile = new File(outFilename);
                String folderName = outFile.getParent();
                Files.createDirectories(Paths.get(folderName));
                StreamResult streamResult = new StreamResult(new File(outFilename));
                transformer.transform(domSource, streamResult);
            }
        }catch(TransformerConfigurationException e) {
            e.printStackTrace();
        }catch(TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
