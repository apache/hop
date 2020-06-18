package org.apache.hop.core.gui.plugin;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class GuiRegistryTest {

  private GuiRegistry registry;

  @Before
  public void before() {
    registry = GuiRegistry.getInstance();
  }

  @Test
  public void retrieveClassInstance() {
    Object object1 = new Object();
    registry.registerGuiPluginObject( "hop-gui-id1", "class1", "instance1", object1 );
    Object verifyObject1111 = registry.findGuiPluginObject( "hop-gui-id1", "class1", "instance1" );
    assertEquals( object1, verifyObject1111 );
    // class2 is not found
    Object verifyObject1211 = registry.findGuiPluginObject( "hop-gui-id1", "class2", "instance1" );
    assertNull(verifyObject1211);
  }

  @Test
  public void retrieveSameObjectMultipleInstances() {
    Object object1 = new Object();
    registry.registerGuiPluginObject( "hop-gui-id1", "class1", "instance1", object1 );
    registry.registerGuiPluginObject( "hop-gui-id1", "class1", "instance2", object1 );

    Object verifyObject1121 = registry.findGuiPluginObject( "hop-gui-id1", "class1", "instance2" );
    assertEquals( object1, verifyObject1121 );
    Object verifyObject1111 = registry.findGuiPluginObject( "hop-gui-id1", "class1", "instance1" );
    assertEquals( object1, verifyObject1111 );
  }


}