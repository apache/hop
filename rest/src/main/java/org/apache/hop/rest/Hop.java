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
 *
 */

package org.apache.hop.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.IRestServicesProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;

/** Singleton class serving as the basis for the Web Application */
public class Hop implements IHasHopMetadataProvider, IRestServicesProvider {
  public static final String OPTION_LOG_LEVEL = "logLevel";
  public static final String OPTION_METADATA_EXPORT_FILE = "metadataExportFile";

  private static Hop instance;

  private final Properties properties;
  private final LoggingObject loggingObject;
  private final IVariables variables;
  private final LogChannel log;

  private MultiMetadataProvider metadataProvider;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static Hop getInstance() {
    if (instance == null) {
      instance = new Hop();
    }
    return instance;
  }

  private Hop() {
    try {
      // Initialize Hop
      //
      System.err.println("=====> STARTING <======");
      HopEnvironment.init();

      loggingObject = new LoggingObject("Hop REST API v1");
      log = new LogChannel("Hop REST");

      // Load configuration details.  We need to know where the metadata is, and so on.
      //
      properties = new Properties();
      String propertyPath;
      try {
        if (StringUtils.isEmpty(System.getProperty("HOP_REST_CONFIG_FOLDER"))) {
          propertyPath = "/config";
        } else {
          propertyPath = System.getProperty("HOP_REST_CONFIG_FOLDER");
        }
        String configFileName = propertyPath + "/hop-rest.properties";
        log.logBasic("The configuration filename we're looking for is: " + configFileName);
        File configFile = new File(configFileName);
        if (configFile.exists()) {
          try (InputStream inputStream = new FileInputStream(configFile)) {
            properties.load(inputStream);
            log.logBasic("Configuration file " + configFileName + " was successfully loaded.");
          }
        } else {
          try (InputStream inputStream = getClass().getResourceAsStream("/hop-rest.properties")) {
            properties.load(inputStream);
          }
        }
      } catch (IOException e) {
        throw new HopException("Error initializing Lean Rest: ", e);
      }

      // Set a logging level for the application
      //
      log.setLogLevel(
          LogLevel.lookupCode(Const.NVL(properties.getProperty(OPTION_LOG_LEVEL), "BASIC")));
      log.logBasic("Starting the Apache Hop REST services application.");

      variables = new Variables();

      // Initialize the logging backend
      //
      HopLogStore.init();

      // Clear the jar file cache so that we don't waste memory...
      //
      JarCache.getInstance().clear();

      // Set up the metadata to use
      //
      metadataProvider = HopMetadataUtil.getStandardHopMetadataProvider(variables);
      HopMetadataInstance.setMetadataProvider(metadataProvider);

      // Allow plugins to modify the elements loaded so far, before a pipeline or workflow is even
      // loaded
      //
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopRestServiceStart.id, this);

      // Optionally we can configure metadata to come from a JSON export file.
      //
      String metadataExportFile = properties.getProperty(OPTION_METADATA_EXPORT_FILE);
      if (StringUtils.isNotEmpty(metadataExportFile)) {
        // Load the JSON from the specified file:
        //
        try (InputStream inputStream = HopVfs.getInputStream(metadataExportFile)) {
          String json = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
          SerializableMetadataProvider exportedProvider = new SerializableMetadataProvider(json);
          metadataProvider.getProviders().add(exportedProvider);

          log.logBasic(
              "Added metadata provider from export file: " + metadataProvider.getDescription());
        }
      }

    } catch (Exception e) {
      throw new RuntimeException("Error initializing the Apache Hop REST services", e);
    }
  }

  /**
   * Sets instance
   *
   * @param instance value of instance
   */
  public static void setInstance(Hop instance) {
    Hop.instance = instance;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * Gets loggingObject
   *
   * @return value of loggingObject
   */
  public LoggingObject getLoggingObject() {
    return loggingObject;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public LogChannel getLog() {
    return log;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override
  public MultiMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * Sets metadataProvider
   *
   * @param metadataProvider value of metadataProvider
   */
  @Override
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  @Override
  public IHasHopMetadataProvider getHasHopMetadataProvider() {
    return this;
  }
}
