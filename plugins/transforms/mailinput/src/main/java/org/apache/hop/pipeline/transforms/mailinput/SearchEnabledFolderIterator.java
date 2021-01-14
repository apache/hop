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

package org.apache.hop.pipeline.transforms.mailinput;

import javax.mail.Message;
import javax.mail.search.SearchTerm;
import java.util.Iterator;

public class SearchEnabledFolderIterator implements Iterator<Message> {

  private Iterator<Message> iterator;
  private SearchTerm searchTerm;

  private Message next;

  public SearchEnabledFolderIterator( Iterator<Message> messageIterator, SearchTerm search ) {
    this.iterator = messageIterator;
    this.searchTerm = search;
    fetchNext();
  }

  @Override
  public boolean hasNext() {
    return next != null && searchTerm.match( next );
  }

  @Override
  public Message next() {
    Message toReturn = next;

    fetchNext();

    return toReturn;
  }

  /**
   * Not implemented.
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void fetchNext() {

    while ( iterator.hasNext() ) {
      next = iterator.next();
      if ( searchTerm.match( next ) ) {
        return;
      } else if ( !iterator.hasNext() ) {
        break;
      }
    }
    next = null;

  }

}
