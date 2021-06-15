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

package org.apache.hop.core.util;

import org.apache.commons.collections4.Closure;
import org.apache.commons.collections4.FunctorException;
import org.apache.hop.core.exception.HopException;
import org.w3c.dom.Node;

import java.util.prefs.Preferences;

public final class PluginPropertyHandler {

  public abstract static class AbstractHandler implements Closure {
    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if property is null.
     * @throws FunctorException         if HopException in handle thrown.
     * @see org.apache.commons.collections.Closure#execute(java.lang.Object)
     */
    public final void execute( final Object property ) throws IllegalArgumentException, FunctorException {
      Assert.assertNotNull( property, "Plugin property cannot be null" );
      try {
        this.handle( (IPluginProperty) property );
      } catch ( HopException e ) {
        throw new FunctorException( "EXCEPTION: " + this, e );
      }
    }

    /**
     * Handle property.
     *
     * @param property property.
     * @throws HopException ...
     */
    protected abstract void handle( final IPluginProperty property ) throws HopException;
  }

  /**
   * <p>
   * Fail/throws HopException.
   */
  public static class Fail extends AbstractHandler {

    /**
     * The message.
     */
    public static final String MESSAGE = "Forced exception";

    /**
     * The instance.
     */
    public static final Fail INSTANCE = new Fail();

    @Override
    protected void handle( final IPluginProperty property ) throws HopException {
      throw new HopException( MESSAGE );
    }

  }

  public static class AppendXml extends AbstractHandler {

    private final StringBuilder builder = new StringBuilder();

    @Override
    protected void handle( final IPluginProperty property ) {
      property.appendXml( this.builder );
    }

    /**
     * @return XML string.
     */
    public String getXml() {
      return this.builder.toString();
    }

  }

  public static class LoadXml extends AbstractHandler {

    private final Node node;

    /**
     * Constructor.
     *
     * @param node node to set.
     * @throws IllegalArgumentException if node is null.
     */
    public LoadXml( final Node node ) throws IllegalArgumentException {
      super();
      Assert.assertNotNull( node, "Node cannot be null" );
      this.node = node;
    }

    @Override
    protected void handle( final IPluginProperty property ) {
      property.loadXml( this.node );
    }

  }

  public static class SaveToPreferences extends AbstractHandler {

    private final Preferences node;

    /**
     * Constructor.
     *
     * @param node node to set.
     * @throws IllegalArgumentException if node is null.
     */
    public SaveToPreferences( final Preferences node ) throws IllegalArgumentException {
      super();
      Assert.assertNotNull( node, "Node cannot be null" );
      this.node = node;
    }

    @Override
    protected void handle( final IPluginProperty property ) {
      property.saveToPreferences( this.node );
    }

  }

  public static class ReadFromPreferences extends AbstractHandler {

    private final Preferences node;

    /**
     * Constructor.
     *
     * @param node node to set.
     * @throws IllegalArgumentException if node is null.
     */
    public ReadFromPreferences( final Preferences node ) throws IllegalArgumentException {
      super();
      Assert.assertNotNull( node, "Node cannot be null" );
      this.node = node;
    }

    @Override
    protected void handle( final IPluginProperty property ) {
      property.readFromPreferences( this.node );
    }

  }

  /**
   * @param properties properties to test.
   * @throws IllegalArgumentException if properties is null.
   */
  public static void assertProperties( final KeyValueSet properties ) throws IllegalArgumentException {
    Assert.assertNotNull( properties, "Properties cannot be null" );
  }

  /**
   * @param properties properties
   * @return XML String
   * @throws IllegalArgumentException if properties is null
   */
  public static String toXml( final KeyValueSet properties ) throws IllegalArgumentException {
    assertProperties( properties );
    final AppendXml handler = new AppendXml();
    properties.walk( handler );
    return handler.getXml();
  }

  /**
   * @param properties properties.
   * @param handler    handler.
   * @throws HopException             ...
   * @throws IllegalArgumentException if properties is null.
   */
  public static void walk( final KeyValueSet properties, final Closure handler ) throws HopException,
    IllegalArgumentException {
    assertProperties( properties );
    try {
      properties.walk( handler );
    } catch ( FunctorException e ) {
      throw (HopException) e.getCause();
    }
  }

  /**
   * Avoid instance creation.
   */
  private PluginPropertyHandler() {
    super();
  }
}
