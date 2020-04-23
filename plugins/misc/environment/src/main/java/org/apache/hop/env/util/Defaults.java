package org.apache.hop.env.util;

import org.apache.hop.core.Const;

import java.io.File;

public class Defaults {
  public static final String VARIABLE_ENVIRONMENT_METASTORE_FOLDER = "ENVIRONMENT_METASTORE_FOLDER";

  public static final String ENVIRONMENT_METASTORE_FOLDER = Const.getHopDirectory()+ File.separator+"environment";

  public static final String VARIABLE_ACTIVE_ENVIRONMENT = "ACTIVE_ENVIRONMENT";

  public static final String EXTENSION_POINT_ENVIRONMENT_ACTIVATED = "EnvironmentActivated";

}
