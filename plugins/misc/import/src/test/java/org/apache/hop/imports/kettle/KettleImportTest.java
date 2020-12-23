package org.apache.hop.imports.kettle;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.imports.HopDbConnImport;
import org.apache.hop.imports.HopVarImport;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.junit.Before;
import org.junit.Test;


public class KettleImportTest {

    private KettleImport kettleImporter;
    private String inputFolderName, outputFolderName, varPath, dbConnPath;


    @Before
    public void setUp(){
        inputFolderName = "src/test/resources/kettle";
        varPath = "src/test/resources/kettle/kettle.properties";
        dbConnPath = "src/test/resources/kettle/shared.xml";
        outputFolderName = "target/hop-imported";

        inputFolderName = "/home/bart/git-customers/drivolution-dcn-auto";
        outputFolderName = "/home/bart/Projects/Hop/migration/dcn-auto";

//        HopEnvironment.init();
        try {
            HopGuiEnvironment.init();
        } catch (HopException e) {
            e.printStackTrace();
        }
        kettleImporter = new KettleImport(inputFolderName, outputFolderName);
    }

    @Test
    public void migrateJobToWorkflow(){
        kettleImporter.importVars(varPath, HopVarImport.PROPERTIES, new Variables());
        kettleImporter.importConnections(dbConnPath, HopDbConnImport.XML);
        kettleImporter.importHopFolder();

        // verify the import was done correctly
    }

}
