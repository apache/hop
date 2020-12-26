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
