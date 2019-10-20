/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.cluster;

import java.nio.charset.Charset;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;

public class BaseCluster extends TestCase {

  public static TransExecutionConfiguration createClusteredTransExecutionConfiguration() {

    TransExecutionConfiguration config = new TransExecutionConfiguration();
    config.setExecutingClustered( true );
    config.setExecutingLocally( false );
    config.setExecutingRemotely( false );
    config.setClusterPosting( true );
    config.setClusterPreparing( true );
    config.setClusterStarting( true );
    config.setLogLevel( LogLevel.MINIMAL );

    // LogWriter.getInstance().setFilter(" server socket ");

    return config;
  }

  public TransMeta loadAndModifyTestTransformation( ClusterGenerator clusterGenerator, String filename ) throws HopException {
    TransMeta transMeta = new TransMeta( filename );

    // Add the slave servers
    //
    for ( SlaveServer slaveServer : ClusterGenerator.LOCAL_TEST_SLAVES ) {
      transMeta.getSlaveServers().add( slaveServer );
    }

    // Replace the slave servers in the specified cluster schema...
    //
    ClusterSchema clusterSchema = transMeta.findClusterSchema( ClusterGenerator.TEST_CLUSTER_NAME );
    assertNotNull( "Cluster schema '" + ClusterGenerator.TEST_CLUSTER_NAME + "' couldn't be found", clusterSchema );
    clusterSchema.getSlaveServers().clear();
    clusterSchema.getSlaveServers().addAll( Arrays.asList( ClusterGenerator.LOCAL_TEST_SLAVES ) );

    return transMeta;
  }

  protected void init() throws Exception {
    // Bootstrap the Hop API...
    //
    HopEnvironment.init();
    HopLogStore.init( 5000, 60 ); // Keep 5000 log rows for at least 60 minutes
  }

  public static String loadFileContent( VariableSpace space, String filename ) throws Exception {
    String realFilename = space.environmentSubstitute( filename );
    return HopVFS.getTextFileContent( realFilename, Charset.defaultCharset().name() );
  }

  protected void assertEqualsIgnoreWhitespaces( String expected, String two ) {
    String oneStripped = stripWhiteSpaces( expected );
    String twoStripped = stripWhiteSpaces( two );

    assertEquals( oneStripped, twoStripped );
  }

  protected void assertEqualsIgnoreWhitespacesAndCase( String expected, String actual ) {
    assertEqualsIgnoreWhitespaces( expected.toUpperCase(), actual.toUpperCase() );
  }

  private String stripWhiteSpaces( String one ) {
    StringBuilder stripped = new StringBuilder();

    boolean previousWhiteSpace = false;

    for ( char c : one.toCharArray() ) {
      if ( Character.isWhitespace( c ) ) {
        if ( !previousWhiteSpace ) {
          stripped.append( ' ' ); // add a single white space, don't add a second
        }
        previousWhiteSpace = true;
      } else {
        if ( c == '('
          || c == ')' || c == '|' || c == '-' || c == '+' || c == '/' || c == '*' || c == '{' || c == '}'
          || c == ',' ) {
          int lastIndex = stripped.length() - 1;
          if ( stripped.charAt( lastIndex ) == ' ' ) {
            stripped.deleteCharAt( lastIndex );
          }
          previousWhiteSpace = true;
        } else {
          previousWhiteSpace = false;
        }
        stripped.append( c );
      }
    }

    // Trim the whitespace (max 1) at the front and back too...
    if ( stripped.length() > 0 && Character.isWhitespace( stripped.charAt( 0 ) ) ) {
      stripped.deleteCharAt( 0 );
    }
    if ( stripped.length() > 0 && Character.isWhitespace( stripped.charAt( stripped.length() - 1 ) ) ) {
      stripped.deleteCharAt( stripped.length() - 1 );
    }

    return stripped.toString();
  }

}
