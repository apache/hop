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

package org.apache.hop.ui.core;

/**
 * Created by bmorrise on 8/17/17.
 */
public class FileDialogOperation {

  public static String SELECT_FOLDER = "selectFolder";
  public static String OPEN = "open";
  public static String SAVE = "save";
  public static String ORIGIN_SPOON = "hopui";
  public static String ORIGIN_OTHER = "other";
  public static String PIPELINE = "pipeline";
  public static String JOB = "workflow";

  private String command;
  private String filter;
  private String origin;
  private String startDir;
  private String title;
  private String filename;
  private String fileType;

  public FileDialogOperation( String command ) {
    this.command = command;
  }

  public FileDialogOperation( String command, String origin ) {
    this.command = command;
    this.origin = origin;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand( String command ) {
    this.command = command;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter( String filter ) {
    this.filter = filter;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin( String origin ) {
    this.origin = origin;
  }

  public String getStartDir() {
    return startDir;
  }

  public void setStartDir( String startDir ) {
    this.startDir = startDir;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle( String title ) {
    this.title = title;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFileType() {
    return fileType;
  }

  public void setFileType( String fileType ) {
    this.fileType = fileType;
  }
}
