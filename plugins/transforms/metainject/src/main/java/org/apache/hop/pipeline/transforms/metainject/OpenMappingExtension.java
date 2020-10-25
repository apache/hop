/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.metainject;


import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.lifecycle.ILifecycleListener;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Created by bmorrise on 7/26/16.
 */
@ExtensionPoint(
  id = "OpenMapping",
  description = "Update Trans Meta based on MetaInjectMeta being the transform type",
  extensionPointId = "OpenMapping" )
public class OpenMappingExtension implements IExtensionPoint {

  //spoon class without import of swt libraries, big-data pmr run doesn't have ui library and shouldn't
  private static final Class<?> PKG = ILifecycleListener.class;

  @Override public void callExtensionPoint(ILogChannel log, Object object ) throws HopException {
    TransformMeta transformMeta = (TransformMeta) ( (Object[]) object )[ 0 ];
    PipelineMeta piprlineMeta = (PipelineMeta) ( (Object[]) object )[ 1 ];

    if ( transformMeta.getTransform() instanceof MetaInjectMeta ) {
      // Make sure we don't accidently overwrite this transformation so we'll remove the filename and objectId
      // Modify the name so the users sees it's a result
      piprlineMeta.setFilename( null );
//      piprlineMeta.setObjectId( null );
      String appendName = " (" + BaseMessages.getString( PKG, "TransGraph.AfterInjection" ) + ")";
      if ( !piprlineMeta.getName().endsWith( appendName ) ) {
        piprlineMeta.setName( piprlineMeta.getName() + appendName );
      }
    }
  }
}
