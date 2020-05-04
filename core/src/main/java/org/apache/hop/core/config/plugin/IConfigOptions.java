package org.apache.hop.core.config.plugin;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.api.IMetaStore;

/**
 * The class implementing this interface has a bunch of Picocli @Option annotations
 * These options have values.
 * Typically this method recognizes the option, takes the arguments it needs and handles the option.
 */
public interface IConfigOptions {

  boolean handleOption( ILogChannel log, IMetaStore metaStore, IVariables variables ) throws HopException;

}
