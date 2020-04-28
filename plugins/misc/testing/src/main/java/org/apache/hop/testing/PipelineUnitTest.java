/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.testing;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.util.HopDefaults;
import org.apache.hop.testing.util.DataSetConst;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class describes a test-case where a pipeline output is verified against golden data.
 *
 * @author matt
 */
@MetaStoreElementType(
  name = "Pipeline Unit Test",
  description = "This describes a test for a pipeline with alternative data sets as input from certain transform and testing output against golden data"
)
public class PipelineUnitTest extends Variables implements IVariables, Cloneable, IHopMetaStoreElement<PipelineUnitTest> {

  private String name;

  @MetaStoreAttribute( key = "description" )
  private String description;

  @MetaStoreAttribute( key = "pipeline_filename" )
  protected String pipelineFilename; // file (3rd priority)

  @MetaStoreAttribute( key = "input_data_sets" )
  protected List<PipelineUnitTestSetLocation> inputDataSets;

  @MetaStoreAttribute( key = "golden_data_sets" )
  protected List<PipelineUnitTestSetLocation> goldenDataSets;

  @MetaStoreAttribute( key = "trans_test_tweaks" )
  protected List<PipelineUnitTestTweak> tweaks;

  @MetaStoreAttribute( key = "test_type" )
  protected TestType type;

  @MetaStoreAttribute( key = "persist_filename" )
  protected String filename;

  @MetaStoreAttribute
  protected String basePath;

  @MetaStoreAttribute( key = "database_replacements" )
  protected List<PipelineUnitTestDatabaseReplacement> databaseReplacements;

  @MetaStoreAttribute
  protected List<VariableValue> variableValues;

  @MetaStoreAttribute
  protected boolean autoOpening;


  public PipelineUnitTest() {
    inputDataSets = new ArrayList<>();
    goldenDataSets = new ArrayList<>();
    tweaks = new ArrayList<>();
    type = TestType.DEVELOPMENT;
    databaseReplacements = new ArrayList<>();
    variableValues = new ArrayList<>();
    basePath = null;
    autoOpening = false;
  }

  public PipelineUnitTest( String name, String description,
                           String pipelineFilename,
                           List<PipelineUnitTestSetLocation> inputDataSets,
                           List<PipelineUnitTestSetLocation> goldenDataSets,
                           List<PipelineUnitTestTweak> tweaks,
                           TestType type,
                           String filename,
                           List<PipelineUnitTestDatabaseReplacement> databaseReplacements,
                           boolean autoOpening ) {
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

  @Override
  public boolean equals( Object obj ) {
    if ( obj == this ) {
      return true;
    }
    if ( !( obj instanceof PipelineUnitTest ) ) {
      return false;
    }
    return ( (PipelineUnitTest) obj ).name.equalsIgnoreCase( name );
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }


  public PipelineUnitTestSetLocation findGoldenLocation( String transformName ) {
    for ( PipelineUnitTestSetLocation location : goldenDataSets ) {
      if ( transformName.equalsIgnoreCase( location.getTransformName() ) ) {
        return location;
      }
    }
    return null;
  }

  public PipelineUnitTestSetLocation findInputLocation( String transformName ) {
    for ( PipelineUnitTestSetLocation location : inputDataSets ) {
      if ( transformName.equalsIgnoreCase( location.getTransformName() ) ) {
        return location;
      }
    }
    return null;
  }

  /**
   * Retrieve the golden data set for the specified location
   *
   * @param log       the logging channel to log to
   * @param metaStore The metaStore to use
   * @param location  the location where we want to check against golden rows
   * @return The golden data set
   * @throws HopException
   */
  public DataSet getGoldenDataSet( ILogChannel log, IMetaStore metaStore, PipelineUnitTestSetLocation location ) throws HopException {

    String transformName = location.getTransformName();
    String goldenDataSetName = location.getDataSetName();

    try {
      // Look in the golden data sets list for the mentioned transform name
      //
      if ( goldenDataSetName == null ) {
        throw new HopException( "Unable to find golden data set for transform '" + transformName + "'" );
      }

      DataSet goldenDataSet = DataSet.createFactory( metaStore ).loadElement( goldenDataSetName );
      if ( goldenDataSet == null ) {
        throw new HopException( "Unable to find golden data set '" + goldenDataSetName + "' for transform '" + transformName + "'" );
      }

      return goldenDataSet;

    } catch ( Exception e ) {
      throw new HopException( "Unable to retrieve sorted golden row data set '" + transformName + "'", e );
    }
  }

  /**
   * Find the first tweak for a certain transform
   *
   * @param transformName the name of the transform on which a tweak is put
   * @return the first tweak for a certain transform or null if nothing was found
   */
  public PipelineUnitTestTweak findTweak( String transformName ) {
    for ( PipelineUnitTestTweak tweak : tweaks ) {
      if ( tweak.getTransformName() != null && tweak.getTransformName().equalsIgnoreCase( transformName ) ) {
        return tweak;
      }
    }
    return null;
  }

  /**
   * Remove all input and golden data sets on the transform with the provided name
   *
   * @param transformName the name of the transform for which we need to clear out all input and golden data sets
   */
  public void removeInputAndGoldenDataSets( String transformName ) {

    for ( Iterator<PipelineUnitTestSetLocation> iterator = inputDataSets.iterator(); iterator.hasNext(); ) {
      PipelineUnitTestSetLocation inputLocation = iterator.next();
      if ( inputLocation.getTransformName().equalsIgnoreCase( transformName ) ) {
        iterator.remove();
      }
    }

    for ( Iterator<PipelineUnitTestSetLocation> iterator = goldenDataSets.iterator(); iterator.hasNext(); ) {
      PipelineUnitTestSetLocation goldenLocation = iterator.next();
      if ( goldenLocation.getTransformName().equalsIgnoreCase( transformName ) ) {
        iterator.remove();
      }
    }
  }

  public boolean matchesPipelineFilename(IVariables variables, String pipelineFilename) throws HopFileException, FileSystemException {
    if ( Utils.isEmpty(pipelineFilename)) {
      return false;
    }
    FileObject pipelineFile = HopVfs.getFileObject( pipelineFilename );
    String pipelineUri = pipelineFile.getName().getURI();

    FileObject testFile = HopVfs.getFileObject( calculateCompleteFilename( variables ) );
    if (!testFile.exists()) {
      return false;
    }
    String testUri = testFile.getName().getURI();

    return pipelineUri.equals( testUri );
  }

  public String calculateCompleteFilename( IVariables space ) {

    String baseFilePath = space.environmentSubstitute( basePath );
    if ( StringUtils.isEmpty( baseFilePath ) ) {
      // See if the base path environment variable is set
      //
      baseFilePath = space.getVariable( DataSetConst.VARIABLE_UNIT_TESTS_BASE_PATH );
    }
    if ( StringUtils.isEmpty( baseFilePath ) ) {
      baseFilePath = "";
    }
    if ( StringUtils.isNotEmpty( baseFilePath ) ) {
      if ( !baseFilePath.endsWith( File.separator ) ) {
        baseFilePath += File.separator;
      }
    }
    return baseFilePath + pipelineFilename;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
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
  public void setDescription( String description ) {
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
  public void setPipelineFilename( String pipelineFilename ) {
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
  public void setInputDataSets( List<PipelineUnitTestSetLocation> inputDataSets ) {
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
  public void setGoldenDataSets( List<PipelineUnitTestSetLocation> goldenDataSets ) {
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
  public void setTweaks( List<PipelineUnitTestTweak> tweaks ) {
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
  public void setType( TestType type ) {
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
  public void setFilename( String filename ) {
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
  public void setBasePath( String basePath ) {
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
  public void setDatabaseReplacements( List<PipelineUnitTestDatabaseReplacement> databaseReplacements ) {
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
  public void setVariableValues( List<VariableValue> variableValues ) {
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
  public void setAutoOpening( boolean autoOpening ) {
    this.autoOpening = autoOpening;
  }

  @Override public MetaStoreFactory<PipelineUnitTest> getFactory( IMetaStore metaStore ) {
    return createFactory( metaStore );
  }

  public static final MetaStoreFactory<PipelineUnitTest> createFactory(IMetaStore metaStore) {
    return new MetaStoreFactory<>( PipelineUnitTest.class, metaStore, HopDefaults.NAMESPACE );
  }
}
