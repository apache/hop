/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.testing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.testing.util.DataSetConst;

/** This class describes a test-case where a pipeline output is verified against golden data. */
@HopMetadata(
    key = "unit-test",
    name = "i18n::PipelineUnitTest.name",
    description = "i18n::PipelineUnitTest.description",
    image = "Test_tube_icon.svg",
    documentationUrl = "/metadata-types/pipeline-unit-test.html",
    hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_UNIT_TEST)
public class PipelineUnitTest extends HopMetadataBase implements Cloneable, IHopMetadata {

  @HopMetadataProperty private String description;

  @HopMetadataProperty(key = "pipeline_filename")
  protected String pipelineFilename; // file (3rd priority)

  @HopMetadataProperty(key = "input_data_sets")
  protected List<PipelineUnitTestSetLocation> inputDataSets;

  @HopMetadataProperty(key = "golden_data_sets")
  protected List<PipelineUnitTestSetLocation> goldenDataSets;

  @HopMetadataProperty(key = "trans_test_tweaks")
  protected List<PipelineUnitTestTweak> tweaks;

  @HopMetadataProperty(key = "test_type")
  protected TestType type;

  @HopMetadataProperty(key = "persist_filename")
  protected String filename;

  @HopMetadataProperty protected String basePath;

  @HopMetadataProperty(key = "database_replacements")
  protected List<PipelineUnitTestDatabaseReplacement> databaseReplacements;

  @HopMetadataProperty protected List<VariableValue> variableValues;

  @HopMetadataProperty protected boolean autoOpening;

  public PipelineUnitTest() {
    inputDataSets = new ArrayList<>();
    goldenDataSets = new ArrayList<>();
    tweaks = new ArrayList<>();
    type = TestType.DEVELOPMENT;
    databaseReplacements = new ArrayList<>();
    variableValues = new ArrayList<>();
    basePath = null;
    autoOpening = true;
  }

  public PipelineUnitTest(
      String name,
      String description,
      String pipelineFilename,
      List<PipelineUnitTestSetLocation> inputDataSets,
      List<PipelineUnitTestSetLocation> goldenDataSets,
      List<PipelineUnitTestTweak> tweaks,
      TestType type,
      String filename,
      List<PipelineUnitTestDatabaseReplacement> databaseReplacements,
      boolean autoOpening) {
    this();
    this.name = name;
    this.description = description;
    this.pipelineFilename = pipelineFilename;
    this.inputDataSets = inputDataSets;
    this.goldenDataSets = goldenDataSets;
    this.tweaks = tweaks;
    this.type = type;
    this.filename = filename;
    this.databaseReplacements = databaseReplacements;
    this.autoOpening = autoOpening;
  }

  public PipelineUnitTestSetLocation findGoldenLocation(String transformName) {
    for (PipelineUnitTestSetLocation location : goldenDataSets) {
      if (transformName.equalsIgnoreCase(location.getTransformName())) {
        return location;
      }
    }
    return null;
  }

  public PipelineUnitTestSetLocation findInputLocation(String transformName) {
    for (PipelineUnitTestSetLocation location : inputDataSets) {
      if (transformName.equalsIgnoreCase(location.getTransformName())) {
        return location;
      }
    }
    return null;
  }

  /**
   * Retrieve the golden data set for the specified location
   *
   * @param log the logging channel to log to
   * @param metadataProvider The metadataProvider to use
   * @param location the location where we want to check against golden rows
   * @return The golden data set
   * @throws HopException
   */
  public DataSet getGoldenDataSet(
      ILogChannel log, IHopMetadataProvider metadataProvider, PipelineUnitTestSetLocation location)
      throws HopException {

    String transformName = location.getTransformName();
    String goldenDataSetName = location.getDataSetName();

    try {
      // Look in the golden data sets list for the mentioned transform name
      //
      if (goldenDataSetName == null) {
        throw new HopException(
            "Unable to find golden data set for transform '" + transformName + "'");
      }

      DataSet goldenDataSet = metadataProvider.getSerializer(DataSet.class).load(goldenDataSetName);
      if (goldenDataSet == null) {
        throw new HopException(
            "Unable to find golden data set '"
                + goldenDataSetName
                + "' for transform '"
                + transformName
                + "'");
      }

      return goldenDataSet;

    } catch (Exception e) {
      throw new HopException(
          "Unable to retrieve sorted golden row data set '" + transformName + "'", e);
    }
  }

  /**
   * Find the first tweak for a certain transform
   *
   * @param transformName the name of the transform on which a tweak is put
   * @return the first tweak for a certain transform or null if nothing was found
   */
  public PipelineUnitTestTweak findTweak(String transformName) {
    for (PipelineUnitTestTweak tweak : tweaks) {
      if (tweak.getTransformName() != null
          && tweak.getTransformName().equalsIgnoreCase(transformName)) {
        return tweak;
      }
    }
    return null;
  }

  /**
   * Remove all input and golden data sets on the transform with the provided name
   *
   * @param transformName the name of the transform for which we need to clear out all input and
   *     golden data sets
   */
  public void removeInputAndGoldenDataSets(String transformName) {

    for (Iterator<PipelineUnitTestSetLocation> iterator = inputDataSets.iterator();
        iterator.hasNext(); ) {
      PipelineUnitTestSetLocation inputLocation = iterator.next();
      if (inputLocation.getTransformName().equalsIgnoreCase(transformName)) {
        iterator.remove();
      }
    }

    for (Iterator<PipelineUnitTestSetLocation> iterator = goldenDataSets.iterator();
        iterator.hasNext(); ) {
      PipelineUnitTestSetLocation goldenLocation = iterator.next();
      if (goldenLocation.getTransformName().equalsIgnoreCase(transformName)) {
        iterator.remove();
      }
    }
  }

  public boolean matchesPipelineFilename(IVariables variables, String referencePipelineFilename)
      throws HopFileException {
    if (Utils.isEmpty(referencePipelineFilename)) {
      return false;
    }
    FileObject pipelineFile = HopVfs.getFileObject(referencePipelineFilename);
    String pipelineUri = pipelineFile.getName().getURI();

    String testPipelineFilename = calculateCompletePipelineFilename(variables);
    if (Utils.isEmpty(testPipelineFilename)) {
      return false;
    }
    FileObject testPipelineFile = HopVfs.getFileObject(testPipelineFilename);
    String testPipelineUri = testPipelineFile.getName().getURI();

    return pipelineUri.equals(testPipelineUri);
  }

  public String calculateCompletePipelineFilename(IVariables variables) {

    // Without a filename we don't have any work
    //
    if (StringUtil.isEmpty(pipelineFilename)) {
      return null;
    }

    // If the filename is an absolute path, just return that.
    //
    if (pipelineFilename.startsWith("/") || pipelineFilename.startsWith("file:///")) {
      return variables.resolve(pipelineFilename); // to make sure
    }

    // We're dealing with a relative path vs the base path
    //
    String baseFilePath = variables.resolve(basePath);
    if (StringUtils.isEmpty(baseFilePath)) {
      // See if the base path environment variable is set
      //
      baseFilePath = variables.getVariable(DataSetConst.VARIABLE_HOP_UNIT_TESTS_FOLDER);
    }
    if (StringUtils.isEmpty(baseFilePath)) {
      baseFilePath = "";
    }
    if (StringUtils.isNotEmpty(baseFilePath)) {
      if (!baseFilePath.endsWith("/") && !baseFilePath.endsWith("\\")) {
        baseFilePath += "/";
      }
    }
    return baseFilePath + pipelineFilename;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets transFilename
   *
   * @return value of transFilename
   */
  public String getPipelineFilename() {
    return pipelineFilename;
  }

  /**
   * @param pipelineFilename The transFilename to set
   */
  public void setPipelineFilename(String pipelineFilename) {
    this.pipelineFilename = pipelineFilename;
  }

  /**
   * Gets inputDataSets
   *
   * @return value of inputDataSets
   */
  public List<PipelineUnitTestSetLocation> getInputDataSets() {
    return inputDataSets;
  }

  /**
   * @param inputDataSets The inputDataSets to set
   */
  public void setInputDataSets(List<PipelineUnitTestSetLocation> inputDataSets) {
    this.inputDataSets = inputDataSets;
  }

  /**
   * Gets goldenDataSets
   *
   * @return value of goldenDataSets
   */
  public List<PipelineUnitTestSetLocation> getGoldenDataSets() {
    return goldenDataSets;
  }

  /**
   * @param goldenDataSets The goldenDataSets to set
   */
  public void setGoldenDataSets(List<PipelineUnitTestSetLocation> goldenDataSets) {
    this.goldenDataSets = goldenDataSets;
  }

  /**
   * Gets tweaks
   *
   * @return value of tweaks
   */
  public List<PipelineUnitTestTweak> getTweaks() {
    return tweaks;
  }

  /**
   * @param tweaks The tweaks to set
   */
  public void setTweaks(List<PipelineUnitTestTweak> tweaks) {
    this.tweaks = tweaks;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public TestType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType(TestType type) {
    this.type = type;
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets basePath
   *
   * @return value of basePath
   */
  public String getBasePath() {
    return basePath;
  }

  /**
   * @param basePath The basePath to set
   */
  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  /**
   * Gets databaseReplacements
   *
   * @return value of databaseReplacements
   */
  public List<PipelineUnitTestDatabaseReplacement> getDatabaseReplacements() {
    return databaseReplacements;
  }

  /**
   * @param databaseReplacements The databaseReplacements to set
   */
  public void setDatabaseReplacements(
      List<PipelineUnitTestDatabaseReplacement> databaseReplacements) {
    this.databaseReplacements = databaseReplacements;
  }

  /**
   * Gets variableValues
   *
   * @return value of variableValues
   */
  public List<VariableValue> getVariableValues() {
    return variableValues;
  }

  /**
   * @param variableValues The variableValues to set
   */
  public void setVariableValues(List<VariableValue> variableValues) {
    this.variableValues = variableValues;
  }

  /**
   * Gets autoOpening
   *
   * @return value of autoOpening
   */
  public boolean isAutoOpening() {
    return autoOpening;
  }

  /**
   * @param autoOpening The autoOpening to set
   */
  public void setAutoOpening(boolean autoOpening) {
    this.autoOpening = autoOpening;
  }

  public void setRelativeFilename(IVariables variables, String referencePipelineFilename)
      throws HopException {
    // Build relative path whenever a pipeline is saved
    //
    if (StringUtils.isEmpty(referencePipelineFilename)) {
      return; // nothing we can do
    }

    // Set the filename to be safe
    //
    setPipelineFilename(referencePipelineFilename);

    String base = getBasePath();
    if (StringUtils.isEmpty(base)) {
      base = variables.getVariable(DataSetConst.VARIABLE_HOP_UNIT_TESTS_FOLDER);
    }
    base = variables.resolve(base);
    if (StringUtils.isNotEmpty(base)) {
      // See if the base path is present in the filename
      // Then replace the filename
      //
      try {
        FileObject baseFolder = HopVfs.getFileObject(base);
        FileObject pipelineFile = HopVfs.getFileObject(referencePipelineFilename);
        FileObject parent = pipelineFile.getParent();
        while (parent != null) {
          if (parent.equals(baseFolder)) {
            // Here we are, we found the base folder in the pipeline file
            //
            String pipelineFileString = pipelineFile.toString();
            String baseFolderName = parent.toString();

            // Final validation & unit test filename correction
            //
            if (pipelineFileString.startsWith(baseFolderName)) {
              String relativeFile = pipelineFileString.substring(baseFolderName.length());
              String relativeFilename;
              if (relativeFile.startsWith("/")) {
                relativeFilename = "." + relativeFile;
              } else {
                relativeFilename = "./" + relativeFile;
              }
              // Set the pipeline filename to the relative path
              //
              setPipelineFilename(relativeFilename);

              LogChannel.GENERAL.logDetailed(
                  "Unit test '"
                      + getName()
                      + "' : saved relative path to pipeline: "
                      + relativeFilename);
            }
          }
          parent = parent.getParent();
        }
      } catch (Exception e) {
        throw new HopException("Error calculating relative unit test file path", e);
      }
    }
  }
}
