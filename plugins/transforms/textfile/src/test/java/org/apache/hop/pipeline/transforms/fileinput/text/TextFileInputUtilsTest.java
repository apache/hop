/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TextFileInputUtilsTest {
  @Test
  public void guessStringsFromLine() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";

    String line = "\"\\\\valueA\"|\"valueB\\\\\"|\"val\\\\ueC\""; // "\\valueA"|"valueB\\"|"val\\ueC"

    String[] strings = TextFileInputUtils
      .guessStringsFromLine( Mockito.mock( IVariables.class ), Mockito.mock( ILogChannel.class ),
        line, inputMeta, "|", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\valueA", strings[ 0 ] );
    Assert.assertEquals( "valueB\\", strings[ 1 ] );
    Assert.assertEquals( "val\\ueC", strings[ 2 ] );
  }

  @Test
  public void convertLineToStrings() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";
    inputMeta.inputFields = new BaseFileField[ 3 ];
    inputMeta.content.escapeCharacter = "\\";

    String line = "\"\\\\fie\\\\l\\dA\"|\"fieldB\\\\\"|\"fie\\\\ldC\""; // ""\\fie\\l\dA"|"fieldB\\"|"Fie\\ldC""

    String[] strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, "|", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\fie\\l\\dA", strings[ 0 ] );
    Assert.assertEquals( "fieldB\\", strings[ 1 ] );
    Assert.assertEquals( "fie\\ldC", strings[ 2 ] );
  }

  @Test
  public void convertCSVLinesToStrings() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";
    inputMeta.inputFields = new BaseFileField[ 2 ];
    inputMeta.content.escapeCharacter = "\\";

    String line = "A\\\\,B"; // A\\,B

    String[] strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\", strings[ 0 ] );
    Assert.assertEquals( "B", strings[ 1 ] );

    line = "\\,AB"; // \,AB

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( ",AB", strings[ 0 ] );
    Assert.assertEquals( null, strings[ 1 ] );

    line = "\\\\\\,AB"; // \\\,AB

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\,AB", strings[ 0 ] );
    Assert.assertEquals( null, strings[ 1 ] );

    line = "AB,\\"; // AB,\

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "AB", strings[ 0 ] );
    Assert.assertEquals( "\\", strings[ 1 ] );

    line = "AB,\\\\\\"; // AB,\\\

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "AB", strings[ 0 ] );
    Assert.assertEquals( "\\\\", strings[ 1 ] );

    line = "A\\B,C"; // A\B,C

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\B", strings[ 0 ] );
    Assert.assertEquals( "C", strings[ 1 ] );
  }

  @Test
  public void convertCSVLinesToStringsWithEnclosure() throws Exception {
    TextFileInputMeta inputMeta = Mockito.mock( TextFileInputMeta.class );
    inputMeta.content = new TextFileInputMeta.Content();
    inputMeta.content.fileType = "CSV";
    inputMeta.inputFields = new BaseFileField[ 2 ];
    inputMeta.content.escapeCharacter = "\\";
    inputMeta.content.enclosure = "\"";

    String line = "\"A\\\\\",\"B\""; // "A\\","B"

    String[] strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\", strings[ 0 ] );
    Assert.assertEquals( "B", strings[ 1 ] );

    line = "\"\\\\\",\"AB\""; // "\\","AB"

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "\\", strings[ 0 ] );
    Assert.assertEquals( "AB", strings[ 1 ] );

    line = "\"A\\B\",\"C\""; // "A\B","C"

    strings = TextFileInputUtils
      .convertLineToStrings( Mockito.mock( ILogChannel.class ), line, inputMeta, ",", "\"", "\\" );
    Assert.assertNotNull( strings );
    Assert.assertEquals( "A\\B", strings[ 0 ] );
    Assert.assertEquals( "C", strings[ 1 ] );
  }

}
