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

package org.apache.hop.core.compress;

import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompressionProviderFactory implements ICompressionProviderFactory {

  protected static CompressionProviderFactory INSTANCE = new CompressionProviderFactory();

  private CompressionProviderFactory() {
  }

  public static CompressionProviderFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public ICompressionProvider createCompressionProviderInstance( String name ) {

    ICompressionProvider provider = null;

    List<IPlugin> providers = getPlugins();
    if ( providers != null ) {
      for ( IPlugin plugin : providers ) {
        if ( name != null && name.equalsIgnoreCase( plugin.getName() ) ) {
          try {
            return PluginRegistry.getInstance().loadClass( plugin, ICompressionProvider.class );
          } catch ( Exception e ) {
            provider = null;
          }
        }
      }
    }

    return provider;
  }

  @Override
  public Collection<ICompressionProvider> getCompressionProviders() {
    Collection<ICompressionProvider> providerClasses = new ArrayList<>();

    List<IPlugin> providers = getPlugins();
    if ( providers != null ) {
      for ( IPlugin plugin : providers ) {
        try {
          providerClasses.add( PluginRegistry.getInstance().loadClass( plugin, ICompressionProvider.class ) );
        } catch ( Exception e ) {
          // Do nothing here, if we can't load the provider, don't add it to the list
        }
      }
    }
    return providerClasses;
  }

  @Override
  public String[] getCompressionProviderNames() {
    ArrayList<String> providerNames = new ArrayList<>();

    List<IPlugin> providers = getPlugins();
    if ( providers != null ) {
      for ( IPlugin plugin : providers ) {
        try {
          ICompressionProvider provider = PluginRegistry.getInstance().loadClass( plugin, ICompressionProvider.class );
          if ( provider != null ) {
            providerNames.add( provider.getName() );
          }
        } catch ( Exception e ) {
          // Do nothing here, if we can't load the provider, don't add it to the list
        }
      }
    }
    return providerNames.toArray( new String[ providerNames.size() ] );
  }

  @Override
  public ICompressionProvider getCompressionProviderByName( String name ) {
    if ( name == null ) {
      return null;
    }
    ICompressionProvider foundProvider = null;
    List<IPlugin> providers = getPlugins();
    if ( providers != null ) {
      for ( IPlugin plugin : providers ) {
        try {
          ICompressionProvider provider = PluginRegistry.getInstance().loadClass( plugin, ICompressionProvider.class );
          if ( provider != null && name.equals( provider.getName() ) ) {
            foundProvider = provider;
          }
        } catch ( Exception e ) {
          // Do nothing here, if we can't load the provider, don't add it to the list
        }
      }
    }
    return foundProvider;
  }

  protected List<IPlugin> getPlugins() {
    return PluginRegistry.getInstance().getPlugins( CompressionPluginType.class );
  }
}
