/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.model;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.git.model.repository.GitRepository;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;

import static org.junit.Assert.assertEquals;

public class GitRepositoryTest {

  public static final String NAME = "test";
  public static final String DESCRIPTION = "test description";
  public static final String DIRECTORY = "/tmp/test";

  protected IHopMetadataProvider metadataProvider;

  @Spy
  protected GitRepository repo;

  @Before
  public void setUp() throws HopException {
    HopClientEnvironment.init();
    metadataProvider = new MemoryMetadataProvider();

    repo = new GitRepository();
    repo.setName( NAME );
    repo.setDescription( DESCRIPTION );
    repo.setDirectory( DIRECTORY );
  }

  @Test
  public void testSerialisation() throws HopException {
    IHopMetadataSerializer<GitRepository> serializer = metadataProvider.getSerializer( GitRepository.class );
    serializer.save( repo );

    GitRepository verify = serializer.load( NAME );
    assertEquals( NAME, verify.getName() );
    assertEquals( DESCRIPTION, verify.getDescription() );
    assertEquals( DIRECTORY, verify.getDirectory() );
  }

  @Test
  public void testVariableSubstitution() {
    IVariables variables = Variables.getADefaultVariableSpace();
    variables.setVariable( "BASE_DIRECTORY", DIRECTORY );
    repo.setDirectory( "${BASE_DIRECTORY}" );

    assertEquals( DIRECTORY, repo.getPhysicalDirectory(variables) );
  }
}
