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

package org.apache.hop.ui.core.widget;

public class UndoRedoStack {

  public static final int DELETE = 0;
  public static final int INSERT = 1;

  private String strNewText;
  private String strReplacedText;
  private int iCursorPosition;
  private int iEventLength;
  private int iType;

  public UndoRedoStack( int iCursorPosition, String strNewText, String strReplacedText, int iEventLength, int iType ) {
    this.iCursorPosition = iCursorPosition;
    this.strNewText = strNewText;
    this.strReplacedText = strReplacedText;
    this.iEventLength = iEventLength;
    this.iType = iType;
  }

  public String getReplacedText() {
    return this.strReplacedText;
  }

  public String getNewText() {
    return this.strNewText;
  }

  public int getCursorPosition() {
    return this.iCursorPosition;
  }

  public int getEventLength() {
    return iEventLength;
  }

  public int getType() {
    return iType;
  }

}
