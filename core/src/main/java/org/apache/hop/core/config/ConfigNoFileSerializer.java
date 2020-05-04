package org.apache.hop.core.config;

import org.apache.hop.core.exception.HopException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConfigNoFileSerializer implements IHopConfigSerializer {
  @Override public void writeToFile( String filename, Map<String, Object> configMap ) throws HopException {
  }

  @Override public Map<String, Object> readFromFile( String filename ) throws HopException {
    return Collections.synchronizedMap( new HashMap<>() );
  }
}
