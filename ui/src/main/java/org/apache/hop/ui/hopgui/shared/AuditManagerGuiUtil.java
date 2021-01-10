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

package org.apache.hop.ui.hopgui.shared;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.ui.core.gui.HopNamespace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for conveniently storing and retrieving items, lists and so on...
 */
public class AuditManagerGuiUtil {

  /**
   * Return the last used value of a certain type.
   * This method looks in the active namespace in HopGui
   * In case there is an error it is simply logged on the UI log channel as it's not THAT important.
   * @param type The type of list to query
   * @return The last used value or "" (empty string) if nothing could be found (or there was an error)
   */
  public static final String getLastUsedValue(String type) {
    // What is the last pipeline execution configuration used for the active namespace in HopGui?
    //
    try {
      AuditList list = AuditManager.getActive().retrieveList( HopNamespace.getNamespace(), type );
      if (list==null || list.getNames()==null || list.getNames().isEmpty()) {
        return "";
      }
      return list.getNames().get( 0 );
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to get last used value from audit manager type: "+type+" in group "+HopNamespace.getNamespace(), e );
      return "";
    }
  }

  /**
   * Return the last used values of a certain type.
   * This method looks in the active namespace in HopGui
   * In case there is an error it is simply logged on the UI log channel as it's not THAT important.
   * @param type The type of list to query
   * @return The last used values or String[0] (empty array) if nothing could be found (or there was an error)
   */
  public static final String[] getLastUsedValues(String type) {
    // What is the last pipeline execution configuration used for the active namespace in HopGui?
    //
    try {
      AuditList list = AuditManager.getActive().retrieveList( HopNamespace.getNamespace(), type );
      if (list==null || list.getNames()==null || list.getNames().isEmpty()) {
        return new String[0];
      }
      return list.getNames().toArray(new String[0]);
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to get last used values from audit manager type: "+type+" in group "+HopNamespace.getNamespace(), e );
      return new String[0];
    }
  }

  public static final void addLastUsedValue(String type, String value) {
    if ( StringUtil.isEmpty(value)) {
      return; // Not storing empty values
    }
    IAuditManager auditManager = AuditManager.getActive();
    try {
      AuditList list = auditManager.retrieveList( HopNamespace.getNamespace(), type );
      if (list==null) {
        list = new AuditList();
      }
      List<String> names = list.getNames();

      // Move the value to the start of the list if it exists
      //
      int index = names.indexOf( value );
      if (index>=0) {
        names.remove( index );
      }
      names.add(0, value);

      // Remove the last items when we have more than 50 in the list // TODO allow this to be configured
      // We don't want these things to grow out of control
      //
      while (list.getNames().size()>50) {
        list.getNames().remove( list.getNames().size()-1 );
      }
      auditManager.storeList( HopNamespace.getNamespace(), type, list );
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to store list using audit manager with type: "+type+" in group "+HopNamespace.getNamespace(), e );
    }
  }

  public static final Map<String, String> getUsageMap(String type) {
    if ( StringUtil.isEmpty(type)) {
      return new HashMap<>();
    }
    IAuditManager auditManager = AuditManager.getActive();
    try {
      Map<String, String> map = auditManager.loadMap( HopNamespace.getNamespace(), type );
      if (map==null) {
        map = new HashMap<>();
      }
      return map;
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to retrieve the usage map for type "+type+" in group "+HopNamespace.getNamespace(), e );
      return new HashMap<>();
    }
  }

  public static final void saveUsageMap(String type, Map<String, String> map) {
    if ( StringUtil.isEmpty(type)) {
      return; // Nothing to store
    }
    IAuditManager auditManager = AuditManager.getActive();
    try {
      auditManager.saveMap( HopNamespace.getNamespace(), type, map );
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to save the usage map for type "+type+" in group "+HopNamespace.getNamespace(), e );
    }
  }
}
