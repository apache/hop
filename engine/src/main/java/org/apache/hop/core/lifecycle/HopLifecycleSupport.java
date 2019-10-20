/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.HopLifecyclePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeListener;
import org.apache.hop.i18n.BaseMessages;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single point of contact for Hop Lifecycle Plugin instances for invoking lifecycle methods.
 */
public class HopLifecycleSupport {
  private static Class<?> PKG = Const.class; // for i18n purposes, needed by Translator2!!
  @VisibleForTesting protected static PluginRegistry registry = PluginRegistry.getInstance();

  private ConcurrentMap<HopLifecycleListener, Boolean> kettleLifecycleListeners;
  private AtomicBoolean initialized = new AtomicBoolean( false );

  public HopLifecycleSupport() {
    Set<HopLifecycleListener> listeners =
      LifecycleSupport.loadPlugins( HopLifecyclePluginType.class, HopLifecycleListener.class );
    kettleLifecycleListeners = new ConcurrentHashMap<HopLifecycleListener, Boolean>();

    for ( HopLifecycleListener kll: listeners ) {
      kettleLifecycleListeners.put( kll, false );
    }

    registry.addPluginListener( HopLifecyclePluginType.class, new PluginTypeListener() {

      @Override
      public void pluginAdded( Object serviceObject ) {
        HopLifecycleListener listener = null;
        try {
          listener = (HopLifecycleListener) registry.loadClass( (PluginInterface) serviceObject );
        } catch ( HopPluginException e ) {
          e.printStackTrace();
          return;
        }
        kettleLifecycleListeners.put( listener, false );
        if ( initialized.get() ) {
          try {
            onEnvironmentInit( listener );
          } catch ( Throwable e ) {
            // Exception is unexpected and couldn't recover
            Throwables.propagate( e );
          }
        }
      }

      @Override
      public void pluginRemoved( Object serviceObject ) {
        kettleLifecycleListeners.remove( serviceObject );
      }

      @Override
      public void pluginChanged( Object serviceObject ) {
      }

    } );
  }

  /**
   * Execute all known listener's {@link #onEnvironmentInit()} methods. If an invocation throws a
   * {@link LifecycleException} is severe this method will re-throw the exception.
   *
   * @throws LifecycleException
   *           if any listener throws a severe Lifecycle Exception or any {@link Throwable}.
   */
  public void onEnvironmentInit() throws HopException {
    // Execute only once
    if ( initialized.compareAndSet( false, true ) ) {
      for ( HopLifecycleListener listener : kettleLifecycleListeners.keySet() ) {
        onEnvironmentInit( listener );
      }
    }
  }

  private void onEnvironmentInit( HopLifecycleListener listener ) throws HopException {
    // Run only once per listener
    if ( kettleLifecycleListeners.replace( listener, false, true ) ) {
      try {
        listener.onEnvironmentInit();
      } catch ( LifecycleException ex ) {
        String message =
          BaseMessages.getString( PKG, "LifecycleSupport.ErrorInvokingHopLifecycleListener", listener );
        if ( ex.isSevere() ) {
          throw new HopException( message, ex );
        }
        // Not a severe error so let's simply log it and continue invoking the others
        LogChannel.GENERAL.logError( message, ex );
      } catch ( Throwable t ) {
        Throwables.propagateIfPossible( t, HopException.class );
        String message = BaseMessages.getString(
          PKG, "LifecycleSupport.ErrorInvokingHopLifecycleListener", listener );
        throw new HopException( message, t );
      }
    }
  }

  public void onEnvironmentShutdown() {
    for ( HopLifecycleListener listener : kettleLifecycleListeners.keySet() ) {
      try {
        listener.onEnvironmentShutdown();
      } catch ( Throwable t ) {
        // Log the error and continue invoking other listeners
        LogChannel.GENERAL.logError( BaseMessages.getString(
          PKG, "LifecycleSupport.ErrorInvokingHopLifecycleListener", listener ), t );
      }
    }
  }

}
