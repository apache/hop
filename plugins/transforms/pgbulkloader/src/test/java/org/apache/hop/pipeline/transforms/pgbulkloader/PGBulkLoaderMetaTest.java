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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by gmoran on 2/25/14.
 */
public class PGBulkLoaderMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private TransformMeta transformMeta;
  private PGBulkLoader loader;
  private PGBulkLoaderData ld;
  private PGBulkLoaderMeta lm;

  LoadSaveTester loadSaveTester;
  Class<PGBulkLoaderMeta> testMetaClass = PGBulkLoaderMeta.class;

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "schemaName", "tableName", "loadAction", "dbNameOverride", "delimiter",
        "enclosure", "stopOnError", "fieldTable", "fieldStream", "dateMask", "databaseMeta" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "schemaName", "getSchemaName" );
        put( "tableName", "getTableName" );
        put( "loadAction", "getLoadAction" );
        put( "dbNameOverride", "getDbNameOverride" );
        put( "delimiter", "getDelimiter" );
        put( "enclosure", "getEnclosure" );
        put( "stopOnError", "isStopOnError" );
        put( "fieldTable", "getFieldTable" );
        put( "fieldStream", "getFieldStream" );
        put( "dateMask", "getDateMask" );
        put( "databaseMeta", "getDatabaseMeta" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "schemaName", "setSchemaName" );
        put( "tableName", "setTableName" );
        put( "loadAction", "setLoadAction" );
        put( "dbNameOverride", "setDbNameOverride" );
        put( "delimiter", "setDelimiter" );
        put( "enclosure", "setEnclosure" );
        put( "stopOnError", "setStopOnError" );
        put( "fieldTable", "setFieldTable" );
        put( "fieldStream", "setFieldStream" );
        put( "dateMask", "setDateMask" );
        put( "databaseMeta", "setDatabaseMeta" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );
    IFieldLoadSaveValidator<String[]> datemaskArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new DateMaskLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fieldTable", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "dateMask", datemaskArrayLoadSaveValidator );
    attrValidatorMap.put( "databaseMeta", new DatabaseMetaLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();
    // typeValidatorMap.put( int[].class.getCanonicalName(), new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator(), 1 ) );

    loadSaveTester = new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Before
  public void setUp() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "loader" );

    lm = new PGBulkLoaderMeta();
    ld = new PGBulkLoaderData();

    PluginRegistry plugReg = PluginRegistry.getInstance();

    String loaderPid = plugReg.getPluginId( TransformPluginType.class, lm );

    transformMeta = new TransformMeta( loaderPid, "loader", lm );
    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );
    pipelineMeta.addTransform( transformMeta );

    loader = new PGBulkLoader( transformMeta, lm, ld, 1, pipelineMeta, pipeline );
  }

  public static class DateMaskLoadSaveValidator implements IFieldLoadSaveValidator<String> {
    Random r = new Random();
    private final String[] masks = new String[] { PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH, PGBulkLoaderMeta.DATE_MASK_DATE, PGBulkLoaderMeta.DATE_MASK_DATETIME };

    @Override
    public String getTestObject() {
      int idx = r.nextInt( 3 );
      return masks[ idx ];
    }

    @Override
    public boolean validateTestObject( String test, Object actual ) {
      return test.equals( actual );
    }
  }
}
