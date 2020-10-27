/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.recordsfromstream;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.pipeline.transforms.rowsfromresult.RowsFromResultMeta;

@Transform(
        id = "RecordsFromStream",
        image = "recordsfromstream.svg",
        i18nPackageName = "org.apache.hop.pipeline.transforms.recordsfromstream",
        name = "RecordsFromStream.Name",
        description = "RecordsFromStream.Description",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/transforms/recordsfromstream.html"
)
public class RecordsFromStreamMeta extends RowsFromResultMeta {
}
