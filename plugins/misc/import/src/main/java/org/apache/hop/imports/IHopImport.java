package org.apache.hop.imports;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;

import java.io.File;
import java.io.IOException;

public interface IHopImport {

    void setInputFolder(String inputFolder);
    String getInputFolder();

    void setOutputFolder(String outputFolder);
    String getOutputFolder();

    void importHopFolder();
    void importHopFile(File fileToImport);

    IVariables importVars(String varPath, HopVarImport varType, IVariables variables) throws IOException;
    void importConnections(String dbConnPath, HopDbConnImport connType);

    void addDatabaseMeta(String filename, DatabaseMeta databaseMeta);
}
