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

package org.apache.hop.pipeline.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.api.IDeclaresLineage;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.pipeline.transforms.csvinput.CsvInputField;
import org.apache.hop.pipeline.transforms.csvinput.CsvInputMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputMeta;
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileField;
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutputMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The files the text file transforms declare, for publishing a pipeline without running it. */
class TextFileLineageDeclarationTest {

  @TempDir Path folder;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  private static IRowMeta row() {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("id"));
    row.addValueMeta(new ValueMetaString("name"));
    return row;
  }

  private static List<String> columns(IDeclaresLineage.Declaration declaration) {
    List<String> names = new ArrayList<>();
    for (FileIoTabularColumn column :
        ((IDeclaresLineage.DeclaredFile) declaration).content().getColumns()) {
      names.add(column.getName() + ":" + column.getHopTypeName());
    }
    return names;
  }

  @Test
  void textFileOutputDeclaresTheFileItWritesWithTheFieldsItWrites() {
    IVariables variables = new Variables();
    variables.setVariable("OUT", "/data/out");
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setDefault();
    meta.getFileSettings().setFileName("${OUT}/orders");
    meta.getFileSettings().setExtension("csv");

    List<IDeclaresLineage.Declaration> declared = meta.declaredLineage(variables, row());

    assertEquals(1, declared.size());
    IDeclaresLineage.DeclaredFile file = (IDeclaresLineage.DeclaredFile) declared.get(0);
    assertEquals(FileIoOperation.WRITE, file.operation());
    assertEquals("/data/out/orders.csv", file.filename());
    assertEquals(List.of("id:Integer", "name:String"), columns(file));

    TextFileField name = new TextFileField();
    name.setName("name");
    name.setType(IValueMeta.TYPE_STRING);
    meta.setOutputFields(new ArrayList<>(List.of(name)));
    assertEquals(List.of("name:String"), columns(meta.declaredLineage(variables, row()).get(0)));

    meta.setFileNameInField(true);
    assertTrue(meta.declaredLineage(variables, row()).isEmpty());
  }

  @Test
  void csvInputDeclaresTheFileItReadsWithItsFields() {
    IVariables variables = new Variables();
    variables.setVariable("IN", "/data/in");
    CsvInputMeta meta = new CsvInputMeta();
    meta.setDefault();
    meta.setFilename("${IN}/orders.csv");
    CsvInputField id = new CsvInputField("id", -1, -1);
    id.setType(IValueMeta.TYPE_INTEGER);
    meta.setInputFields(new ArrayList<>(List.of(id)));

    List<IDeclaresLineage.Declaration> declared = meta.declaredLineage(variables, new RowMeta());

    IDeclaresLineage.DeclaredFile file = (IDeclaresLineage.DeclaredFile) declared.get(0);
    assertEquals(FileIoOperation.READ, file.operation());
    assertEquals("/data/in/orders.csv", file.filename());
    assertEquals(List.of("id:Integer"), columns(file));

    meta.setFilenameField("filename");
    assertTrue(meta.declaredLineage(variables, new RowMeta()).isEmpty());
  }

  @Test
  void textFileInputDeclaresTheFilesItWouldReadNow() throws Exception {
    Path csv = Files.writeString(folder.resolve("customers.csv"), "id;name\n1;Ann\n");
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.setDefault();
    InputFile input = new InputFile();
    input.setFileName(csv.toString());
    meta.getFileInput().getInputFiles().add(input);
    TextFileInputField id = new TextFileInputField("id", 0, 3);
    id.setType(IValueMeta.TYPE_INTEGER);
    meta.getInputFields().add(id);

    List<IDeclaresLineage.Declaration> declared = meta.declaredLineage(new Variables(), null);

    assertEquals(1, declared.size());
    IDeclaresLineage.DeclaredFile file = (IDeclaresLineage.DeclaredFile) declared.get(0);
    assertEquals(FileIoOperation.READ, file.operation());
    assertTrue(file.filename().endsWith("customers.csv"), file.filename());
    assertEquals(List.of("id:Integer"), columns(file));

    meta.getFileInput().setAcceptingFilenames(true);
    assertTrue(meta.declaredLineage(new Variables(), null).isEmpty());
  }
}
