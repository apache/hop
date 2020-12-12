package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.testing.util.DataSetConst;

import java.util.Map;

@ExtensionPoint(
  id = "TestingVariablesControlSpaceSortOrderPrefix",
  extensionPointId = "HopGuiGetControlSpaceSortOrderPrefix",
  description = "Set a prefix sort order for the testing variables, push fairly back"
)
public class TestingVariablesControlSpaceSortOrderPrefix implements IExtensionPoint<Map<String,String>> {
  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, Map<String, String> prefixMap ) throws HopException {

    prefixMap.put( DataSetConst.VAR_UNIT_TEST_NAME, "450_" );

  }
}
