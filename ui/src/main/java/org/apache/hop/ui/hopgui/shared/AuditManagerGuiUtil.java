package org.apache.hop.ui.hopgui.shared;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.List;

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
    HopGui hopGui = HopGui.getInstance();
    try {
      AuditList list = hopGui.getAuditManager().retrieveList( hopGui.getNamespace(), type );
      if (list==null || list.getNames()==null || list.getNames().isEmpty()) {
        return "";
      }
      return list.getNames().get( 0 );
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to get list from audit manager type: "+type+" in group "+hopGui.getNamespace(), e );
      return "";
    }
  }

  public static final void addLastUsedValue(String type, String value) {
    if ( StringUtil.isEmpty(value)) {
      return; // Not storing empty values
    }
    HopGui hopGui = HopGui.getInstance();
    IAuditManager auditManager = hopGui.getAuditManager();
    try {
      AuditList list = auditManager.retrieveList( hopGui.getNamespace(), type );
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

      // Remove the last items when we have more than 20 in the list // TODO allow this to be configured
      // We don't want these things to grow out of control
      //
      while (list.getNames().size()>20) {
        list.getNames().remove( list.getNames().size()-1 );
      }
      auditManager.storeList( list );
    } catch(Exception e) {
      LogChannel.UI.logError( "Unable to store list using audit manager with type: "+type+" in group "+hopGui.getNamespace(), e );
    }
  }
}
