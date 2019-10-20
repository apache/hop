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

package org.apache.hop.trans.steps.getrepositorynames;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.apache.hop.trans.steps.getrepositorynames.ObjectTypeSelection.All;
import static org.apache.hop.trans.steps.getrepositorynames.ObjectTypeSelection.Jobs;
import static org.apache.hop.trans.steps.getrepositorynames.ObjectTypeSelection.Transformations;

import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.job.JobMeta;
import org.apache.hop.repository.IUser;
import org.apache.hop.repository.Repository;
import org.apache.hop.repository.RepositoryDirectory;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.RepositoryElementMetaInterface;
import org.apache.hop.repository.RepositoryExtended;
import org.apache.hop.repository.RepositoryMeta;
import org.apache.hop.repository.RepositoryObject;
import org.apache.hop.repository.RepositoryObjectType;
import org.apache.hop.repository.filerep.HopFileRepository;
import org.apache.hop.repository.filerep.HopFileRepositoryMeta;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;

public class GetRepositoryNamesTest {

  static Path baseDirName;
  static Repository repo;
  static RepositoryExtended repoExtended;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException, IOException {
    prepareFileRepository();
    prepareExtendedRepository();
  }

  private static void prepareFileRepository() throws IOException, HopException {
    baseDirName = Files.createTempDirectory( "GetRepositoryNamesIT" );
    RepositoryMeta repoMeta =
        new HopFileRepositoryMeta( UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID()
            .toString(), baseDirName.toString() );
    repo = new HopFileRepository();
    repo.init( repoMeta );
    repo.connect( null, null );

    // Populate
    RepositoryDirectoryInterface rootDir = repo.findDirectory( "/" );

    RepositoryDirectoryInterface subdir1 = new RepositoryDirectory( rootDir, "subdir1" );
    repo.saveRepositoryDirectory( subdir1 );

    TransMeta transMeta1 = new TransMeta();
    transMeta1.setName( "Trans1" );
    transMeta1.setRepositoryDirectory( subdir1 );
    repo.save( transMeta1, null, null );

    JobMeta jobMeta1 = new JobMeta();
    jobMeta1.setName( "Job1" );
    jobMeta1.setRepositoryDirectory( subdir1 );
    repo.save( jobMeta1, null, null );

    RepositoryDirectoryInterface subdir2 = new RepositoryDirectory( subdir1, "subdir2" );
    repo.saveRepositoryDirectory( subdir2 );

    TransMeta transMeta2 = new TransMeta();
    transMeta2.setName( "Trans2" );
    transMeta2.setRepositoryDirectory( subdir2 );
    repo.save( transMeta2, null, null );

    JobMeta jobMeta2 = new JobMeta();
    jobMeta2.setName( "Job2" );
    jobMeta2.setRepositoryDirectory( subdir2 );
    repo.save( jobMeta2, null, null );
  }

  @SuppressWarnings( "deprecation" )
  private static void prepareExtendedRepository() throws HopException {
    repoExtended = mock( RepositoryExtended.class );
    when( repoExtended.loadRepositoryDirectoryTree( anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(),
        anyBoolean() ) ).then( new Answer<RepositoryDirectoryInterface>() {
          @Override
          public RepositoryDirectoryInterface answer( InvocationOnMock invocation ) throws Throwable {
            Object[] args = invocation.getArguments();

            RepositoryDirectoryInterface root = new RepositoryDirectory();
            root.setName( "/" );
            RepositoryDirectoryInterface subdir1 = new RepositoryDirectory( root, "subdir1" );
            RepositoryDirectoryInterface subdir2 = new RepositoryDirectory( subdir1, "subdir2" );
            RepositoryElementMetaInterface trans1 =
                new RepositoryObject( null, "Trans1", subdir1, "user", null, RepositoryObjectType.TRANSFORMATION, "",
                    false );
            RepositoryElementMetaInterface trans2 =
                new RepositoryObject( null, "Trans2", subdir2, "user", null, RepositoryObjectType.TRANSFORMATION, "",
                    false );
            RepositoryElementMetaInterface job1 =
                new RepositoryObject( null, "Job1", subdir1, "user", null, RepositoryObjectType.JOB, "", false );
            RepositoryElementMetaInterface job2 =
                new RepositoryObject( null, "Job2", subdir2, "user", null, RepositoryObjectType.JOB, "", false );

            List<RepositoryElementMetaInterface> list1 = new ArrayList<RepositoryElementMetaInterface>();
            List<RepositoryElementMetaInterface> list2 = new ArrayList<RepositoryElementMetaInterface>();
            if ( ( (String) args[1] ).contains( "ktr" ) ) {
              list1.add( trans1 );
              list2.add( trans2 );
            }
            if ( ( (String) args[1] ).contains( "kjb" ) ) {
              list1.add( job1 );
              list2.add( job2 );
            }
            subdir1.setRepositoryObjects( list1 );
            subdir2.setRepositoryObjects( list2 );

            if ( ( (Integer) args[2] ) == -1 ) {
              subdir1.addSubdirectory( subdir2 );
              root.addSubdirectory( subdir1 );
            }
            String actualPath = ( (String) args[0] );
            if ( actualPath.equals( "/" ) ) {
              return root;
            } else if ( actualPath.equals( subdir1.getPath() ) ) {
              return subdir1;
            } else if ( actualPath.equals( subdir2.getPath() ) ) {
              return subdir2;
            } else {
              return null;
            }
          }
        } );

    IUser user = Mockito.mock( IUser.class );
    Mockito.when( user.isAdmin() ).thenReturn( true );
    Mockito.when( repoExtended.getUserInfo() ).thenReturn( user );
  }

  @AfterClass
  public static void tearDownAfterClass() throws HopException, IOException {
    if ( repo != null ) {
      repo.disconnect();
    }
    FileUtils.forceDelete( new File( baseDirName.toString() ) );
  }

  @Test
  public void testGetRepoList_includeSubfolders() throws HopException {
    init( repo, "/", true, ".*", "", All, 4 );
  }

  @Test
  public void testGetRepoList_excludeSubfolders() throws HopException {
    init( repo, "/", false, ".*", "", All, 0 );
  }

  @Test
  public void testGetRepoList_transOnly() throws HopException {
    init( repo, "/", true, ".*", "", Transformations, 2 );
  }

  @Test
  public void testGetRepoList_jobsOnly() throws HopException {
    init( repo, "/", true, ".*", "", Jobs, 2 );
  }

  @Test
  public void testGetRepoList_nameMask() throws HopException {
    init( repo, "/", true, "Trans.*", "", All, 2 );
  }

  @Test
  public void testGetRepoList_withoutNameMask() throws HopException {
    init( repo, "/", true, "", "", All, 4 );
  }

  @Test
  public void testGetRepoList_excludeNameMask() throws HopException {
    init( repo, "/", true, ".*", "Trans1.*", All, 3 );
  }

  @Test
  public void testGetRepoList_includeSubfolders_Extended() throws HopException {
    init( repoExtended, "/", true, ".*", "", All, 4 );
  }

  @Test
  public void testGetRepoList_excludeSubfolders_Extended() throws HopException {
    init( repoExtended, "/", false, ".*", "", All, 0 );
  }

  @Test
  public void testGetRepoList_transOnly_Extended() throws HopException {
    init( repoExtended, "/", true, ".*", "", Transformations, 2 );
  }

  @Test
  public void testGetRepoList_jobsOnly_Extended() throws HopException {
    init( repoExtended, "/", true, ".*", "", Jobs, 2 );
  }

  @Test
  public void testGetRepoList_nameMask_Extended() throws HopException {
    init( repoExtended, "/", true, "Trans.*", "", All, 2 );
  }

  @Test
  public void testGetRepoList_withoutNameMask_Extended() throws HopException {
    init( repoExtended, "/", true, "", "", All, 4 );
  }

  @Test
  public void testGetRepoList_excludeNameMask_Extended() throws HopException {
    init( repoExtended, "/", true, ".*", "Trans1.*", All, 3 );
  }

  @Test
  //PDI-16258
  public void testShowHidden() throws HopException {
    IUser user = Mockito.mock( IUser.class );
    Mockito.when( user.isAdmin() ).thenReturn( true );
    Mockito.when( repoExtended.getUserInfo() ).thenReturn( user );
    init( repoExtended, "/", false, ".*", "", All, 0 );
    Mockito.verify( repoExtended, Mockito.never() )
      .loadRepositoryDirectoryTree( Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.eq( false ),
        Mockito.anyBoolean(), anyBoolean() );

    Mockito.when( user.isAdmin() ).thenReturn( false );
    init( repoExtended, "/", false, ".*", "", All, 0 );
    Mockito.verify( repoExtended )
      .loadRepositoryDirectoryTree( Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.eq( false ),
        Mockito.anyBoolean(), Mockito.anyBoolean() );
  }

  private void init( Repository repository, String directoryName, boolean includeSubFolders, String nameMask, String exludeNameMask,
      ObjectTypeSelection typeSelection, int itemCount ) throws HopException {

    VariableSpace vars = new Variables();
    vars.setVariable( "DirName", "/subdir1" );
    vars.setVariable( "IncludeMask", ".*" );
    vars.setVariable( "ExcludeMask", "" );

    GetRepositoryNamesMeta meta = new GetRepositoryNamesMeta();
    meta.setDirectory( new String[] { directoryName } );
    meta.setNameMask( new String[] { nameMask } );
    meta.setExcludeNameMask( new String[] { exludeNameMask } );
    meta.setIncludeSubFolders( new boolean[] { includeSubFolders } );
    meta.setObjectTypeSelection( typeSelection );
    StepMeta stepMeta = new StepMeta( "GetRepoNamesStep", meta );

    TransMeta transMeta = new TransMeta( vars );
    transMeta.setRepository( repository );
    transMeta.addStep( stepMeta );

    GetRepositoryNamesData data = (GetRepositoryNamesData) meta.getStepData();
    GetRepositoryNames step = new GetRepositoryNames( stepMeta, data, 0, transMeta, new Trans( transMeta ) );
    step.init( meta, data );
    assertNotNull( data.list );
    assertEquals( itemCount, data.list.size() );
  }
}
