/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.repository.kdr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.RepositoryObject;
import org.apache.hop.repository.RepositoryObjectType;
import org.apache.hop.repository.StringObjectId;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryDatabaseDelegate;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryJobDelegate;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryTransDelegate;

/**
 * @author Andrey Khayrutdinov
 */
public class HopDatabaseRepository_GetObjectInformation_Test {
  private static final String ABSENT_ID = "non-existing object";
  private static final String EXISTING_ID = "existing object";
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }


  private HopDatabaseRepository repository;
  private RepositoryDirectoryInterface directoryInterface;

  @Before
  public void setUp() throws Exception {
    directoryInterface = mock( RepositoryDirectoryInterface.class );

    repository = spy( new HopDatabaseRepository() );
    doReturn( directoryInterface ).when( repository ).loadRepositoryDirectoryTree();
    doReturn( directoryInterface )
      .when( repository )
      .loadRepositoryDirectoryTree( any( RepositoryDirectoryInterface.class ) );
  }


  @Test
  public void getObjectInformation_AbsentJob_IsDeletedFlagSet() throws Exception {
    HopDatabaseRepositoryJobDelegate jobDelegate =
      spy( new HopDatabaseRepositoryJobDelegate( repository ) );

    RowMeta meta = createMetaForJob();
    doReturn( new RowMetaAndData( meta, new Object[ meta.size() ] ) )
      .when( jobDelegate )
      .getJob( new StringObjectId( ABSENT_ID ) );

    assertIsDeletedSet_ForAbsentObject( null, jobDelegate, RepositoryObjectType.JOB );
  }

  @Test
  public void getObjectInformation_AbsentTrans_IsDeletedFlagSet() throws Exception {
    HopDatabaseRepositoryTransDelegate transDelegate =
      spy( new HopDatabaseRepositoryTransDelegate( repository ) );

    RowMeta meta = createMetaForTrans();
    doReturn( new RowMetaAndData( meta, new Object[ meta.size() ] ) )
      .when( transDelegate )
      .getTransformation( new StringObjectId( ABSENT_ID ) );

    assertIsDeletedSet_ForAbsentObject( transDelegate, null, RepositoryObjectType.TRANSFORMATION );
  }

  private void assertIsDeletedSet_ForAbsentObject( HopDatabaseRepositoryTransDelegate transDelegate,
                                                   HopDatabaseRepositoryJobDelegate jobDelegate,
                                                   RepositoryObjectType objectType )
    throws Exception {
    repository.transDelegate = transDelegate;
    repository.jobDelegate = jobDelegate;

    when( directoryInterface.findDirectory( any( ObjectId.class ) ) ).thenReturn( null );

    RepositoryObject object = repository.getObjectInformation( new StringObjectId( ABSENT_ID ), objectType );
    assertTrue( object.isDeleted() );
  }


  @Test
  public void getObjectInformation_ExistingJob_IsDeletedFlagNotSet() throws Exception {
    HopDatabaseRepositoryJobDelegate jobDelegate =
      spy( new HopDatabaseRepositoryJobDelegate( repository ) );

    RowMeta meta = createMetaForJob();
    Object[] values = new Object[ meta.size() ];
    values[ Arrays.asList( meta.getFieldNames() ).indexOf( HopDatabaseRepositoryBase.FIELD_JOB_NAME ) ] = EXISTING_ID;
    doReturn( new RowMetaAndData( meta, values ) )
      .when( jobDelegate )
      .getJob( new StringObjectId( EXISTING_ID ) );

    assertIsDeletedNotSet_ForExistingObject( null, jobDelegate, RepositoryObjectType.JOB );
  }

  @Test
  public void getObjectInformation_ExistingTrans_IsDeletedFlagNotSet() throws Exception {
    HopDatabaseRepositoryTransDelegate transDelegate =
      spy( new HopDatabaseRepositoryTransDelegate( repository ) );

    RowMeta meta = createMetaForJob();
    Object[] values = new Object[ meta.size() ];
    values[ Arrays.asList( meta.getFieldNames() ).indexOf( HopDatabaseRepositoryBase.FIELD_TRANSFORMATION_NAME ) ] = EXISTING_ID;
    doReturn( new RowMetaAndData( meta, values ) )
      .when( transDelegate )
      .getTransformation( new StringObjectId( EXISTING_ID ) );

    assertIsDeletedNotSet_ForExistingObject( transDelegate, null, RepositoryObjectType.TRANSFORMATION );
  }

  @Test
  public void getObjectInformation_GetDatabaseInformation() throws Exception {
    HopDatabaseRepositoryDatabaseDelegate databaseDelegate =
        spy( new HopDatabaseRepositoryDatabaseDelegate( repository ) );
    repository.databaseDelegate = databaseDelegate;
    RowMeta meta = createMetaForDatabase();
    Object[] values = new Object[ meta.size() ];
    values[ Arrays.asList( meta.getFieldNames() ).indexOf( HopDatabaseRepositoryBase.FIELD_DATABASE_NAME ) ] = EXISTING_ID;
    doReturn( new RowMetaAndData( meta, values ) )
      .when( databaseDelegate )
      .getDatabase( new StringObjectId( EXISTING_ID ) );
    RepositoryObject actual = repository.getObjectInformation( new StringObjectId( EXISTING_ID ), RepositoryObjectType.DATABASE );

    assertEquals( new StringObjectId( EXISTING_ID ), actual.getObjectId() );
    assertEquals( EXISTING_ID, actual.getName() );
    assertEquals( RepositoryObjectType.DATABASE, actual.getObjectType() );
  }

  private void assertIsDeletedNotSet_ForExistingObject( HopDatabaseRepositoryTransDelegate transDelegate,
                                                        HopDatabaseRepositoryJobDelegate jobDelegate,
                                                        RepositoryObjectType objectType )
    throws Exception {
    repository.transDelegate = transDelegate;
    repository.jobDelegate = jobDelegate;

    when( directoryInterface.findDirectory( any( ObjectId.class ) ) ).thenReturn( null );

    RepositoryObject object = repository.getObjectInformation( new StringObjectId( EXISTING_ID ), objectType );
    assertFalse( object.isDeleted() );
  }


  private static RowMeta createMetaForJob() throws Exception {
    LinkedHashMap<String, Integer> fields = new LinkedHashMap<String, Integer>();
    fields.put( HopDatabaseRepositoryBase.FIELD_JOB_NAME, ValueMetaInterface.TYPE_STRING );
    fields.put( HopDatabaseRepositoryBase.FIELD_JOB_DESCRIPTION, ValueMetaInterface.TYPE_STRING );
    fields.put( HopDatabaseRepositoryBase.FIELD_JOB_MODIFIED_USER, ValueMetaInterface.TYPE_STRING );
    fields.put( HopDatabaseRepositoryBase.FIELD_JOB_MODIFIED_DATE, ValueMetaInterface.TYPE_DATE );
    fields.put( HopDatabaseRepositoryBase.FIELD_JOB_ID_DIRECTORY, ValueMetaInterface.TYPE_INTEGER );
    return createMeta( fields );
  }

  private static RowMeta createMetaForTrans() throws Exception {
    LinkedHashMap<String, Integer> fields = new LinkedHashMap<String, Integer>();
    fields.put( HopDatabaseRepositoryBase.FIELD_TRANSFORMATION_NAME, ValueMetaInterface.TYPE_STRING );
    fields.put( HopDatabaseRepositoryBase.FIELD_TRANSFORMATION_DESCRIPTION, ValueMetaInterface.TYPE_STRING );
    fields.put( HopDatabaseRepositoryBase.FIELD_TRANSFORMATION_MODIFIED_USER, ValueMetaInterface.TYPE_STRING );
    fields.put( HopDatabaseRepositoryBase.FIELD_TRANSFORMATION_MODIFIED_DATE, ValueMetaInterface.TYPE_DATE );
    fields.put( HopDatabaseRepositoryBase.FIELD_TRANSFORMATION_ID_DIRECTORY, ValueMetaInterface.TYPE_INTEGER );
    return createMeta( fields );
  }

  private static RowMeta createMetaForDatabase() throws Exception {
    LinkedHashMap<String, Integer> fields = new LinkedHashMap<String, Integer>();
    fields.put( HopDatabaseRepositoryBase.FIELD_DATABASE_NAME, ValueMetaInterface.TYPE_STRING );
    return createMeta( fields );
  }

  private static RowMeta createMeta( LinkedHashMap<String, Integer> fields ) throws Exception {
    RowMeta meta = new RowMeta();
    for ( Map.Entry<String, Integer> entry : fields.entrySet() ) {
      meta.addValueMeta( ValueMetaFactory.createValueMeta( entry.getKey(), entry.getValue() ) );
    }
    return meta;
  }
}
