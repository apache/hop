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

package org.apache.hop.ui.repository.repositoryexplorer.model;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.repository.RepositoryDirectory;
import org.apache.hop.repository.RepositoryElementMetaInterface;

public class UITransformation extends UIRepositoryContent {

  private static final long serialVersionUID = 3826725834758429573L;

  private static final String REPOSITORY_PKG = "org.apache.hop.ui.repository";

  public UITransformation() {
  }

  public UITransformation( RepositoryElementMetaInterface rc, UIRepositoryDirectory parent, Repository rep ) {
    super( rc, parent, rep );
  }

  @Override
  public void setName( String name ) throws Exception {
    renameTransformation( this.getObjectId(), getRepositoryDirectory(), name );
    super.setName( name );
    uiParent.fireCollectionChanged();
  }

  protected ObjectId renameTransformation( ObjectId objectId, RepositoryDirectory directory, String name )
    throws Exception {
    String comment = BaseMessages.getString( REPOSITORY_PKG, "Repository.Rename", super.getName(), name );
    return rep.renameTransformation( this.getObjectId(), comment, getRepositoryDirectory(), name );
  }

  public void delete() throws Exception {
    rep.deleteTransformation( this.getObjectId() );
    if ( uiParent.getRepositoryObjects().contains( this ) ) {
      uiParent.getRepositoryObjects().remove( this );
    }
  }

  public void move( UIRepositoryDirectory newParentDir ) throws HopException {
    if ( newParentDir != null ) {
      rep.renameTransformation( obj.getObjectId(), newParentDir.getDirectory(), null );
      newParentDir.refresh();
    }
  }

  @Override
  public String getImage() {
    return "ui/images/transrepo.svg";
  }
}
