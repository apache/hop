/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.version;

import org.apache.hop.core.exception.HopVersionException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.xml.XmlHandler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Singleton class to allow us to see on which date & time the kettle3.jar was built.
 *
 * @author Matt
 * @since 2006-aug-12
 */
public class BuildVersion {
  public static final String JAR_BUILD_DATE_FORMAT = "yyyy-MM-dd HH.mm.ss";

  public static final String HOP_BUILD_VERSION = "HOP_BUILD_VERSION";

  public static final String HOP_BUILD_REVISION = "HOP_BUILD_REVISION";

  public static final String HOP_BUILD_DATE = "HOP_BUILD_DATE";

  public static final String HOP_BUILD_USER = "HOP_BUILD_USER";

  protected static ManifestGetter manifestGetter = new ManifestGetter();

  protected static EnvironmentVariableGetter environmentVariableGetter = new EnvironmentVariableGetter();

  private static BuildVersion buildVersion = new BuildVersion();

  /**
   * @return the instance of the BuildVersion singleton
   */
  public static final BuildVersion getInstance() {
    return buildVersion;
  }

  protected static void refreshInstance() {
    buildVersion = new BuildVersion();
  }

  private String version;
  private String revision;
  private String buildDate;
  private String buildUser;

  private void loadBuildInfoFromManifest() throws Exception {
    Manifest manifest = manifestGetter.getManifest();

    version = manifest.getMainAttributes().getValue( Attributes.Name.IMPLEMENTATION_VERSION );
    revision = manifest.getMainAttributes().getValue( Attributes.Name.SPECIFICATION_VERSION );
    buildDate = manifest.getMainAttributes().getValue( "Compile-Timestamp" );
    buildUser = manifest.getMainAttributes().getValue( "Compile-User" );
    if ( version == null ) {
      throw new Exception( "Error:  Version can't be NULL in manifest." );
    }
  }

  private void loadBuildInfoFromEnvironmentVariables() throws Exception {
    version = environmentVariableGetter.getEnvVarible( "HOP_BUILD_VERSION" );
    revision = environmentVariableGetter.getEnvVarible( "HOP_BUILD_REVISION" );
    buildDate = environmentVariableGetter.getEnvVarible( "HOP_BUILD_DATE" );
    buildUser = environmentVariableGetter.getEnvVarible( "HOP_BUILD_USER" );
    if ( version == null ) {
      throw new Exception( "Error : Version can't be null in environment variables" );
    }
  }

  private BuildVersion() {
    try {
      loadBuildInfoFromManifest();
    } catch ( Throwable e ) {
      try {
        loadBuildInfoFromEnvironmentVariables();
      } catch ( Throwable e2 ) {
        version = "Unknown";
        revision = "0";
        buildDate = XmlHandler.date2string( new Date() );
        buildUser = System.getProperty( "user.name" );
      }
    }
  }

  /**
   * @return the buildDate
   */
  public String getBuildDate() {
    return buildDate;
  }

  public Date getBuildDateAsLocalDate() {

    SimpleDateFormat sdf = new SimpleDateFormat( JAR_BUILD_DATE_FORMAT );
    try {
      Date d = sdf.parse( buildDate );
      return d;
      // ignore failure, retry using standard format
    } catch ( ParseException e ) {
      // Ignore
    }

    sdf = new SimpleDateFormat( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
    try {
      Date d = sdf.parse( buildDate );
      return d;
      // ignore failure and return null
    } catch ( ParseException e ) {
      // Ignore
    }

    return null;

  }

  /**
   * @param buildDate the buildDate to set
   */
  public void setBuildDate( String buildDate ) throws HopVersionException {
    // don't let them set a bogus date...
    String tempDate = this.buildDate;
    this.buildDate = buildDate;
    Date testDate = getBuildDateAsLocalDate();
    if ( testDate == null ) {
      // reset it to the old date...
      this.buildDate = tempDate;
      throw new HopVersionException( "Error:  Invalid date being set as build date" ); // this should be
      // localizable... next
      // pass....
    }
  }

  /**
   * @return the version
   */
  public String getVersion() {
    return version;
  }

  /**
   * @param revision the version to set
   */
  public void setVersion( String version ) {
    this.version = version;
  }

  /**
   * @return the revision
   */
  public String getRevision() {
    return revision;
  }

  /**
   * @param revision the revision to set
   */
  public void setRevision( String revision ) {
    this.revision = revision;
  }

  /**
   * @return the buildUser
   */
  public String getBuildUser() {
    return buildUser;
  }

  /**
   * @param buildUser the buildUser to set
   */
  public void setBuildUser( String buildUser ) {
    this.buildUser = buildUser;
  }
}
