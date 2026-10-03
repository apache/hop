/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.perspective.metadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

/**
 * Landing page shown in the metadata perspective whenever no editor tab is open. It presents the
 * full catalog of metadata types (including types that have no items yet, which are hidden in the
 * tree) grouped by category, each with its description, item count and a "New" button. It therefore
 * doubles as the discovery surface for the perspective.
 */
public class MetadataOverview extends Composite {

  private static final Class<?> PKG = MetadataPerspective.class; // i18n

  /** Immutable view of a single metadata type, used to render one card. */
  public record TypeCard(
      String categoryId,
      String key,
      String name,
      String description,
      String image,
      ClassLoader imageClassLoader,
      int count) {}

  private final MetadataPerspective perspective;
  private final ScrolledComposite scrolledComposite;
  private Composite content;

  public MetadataOverview(Composite parent, MetadataPerspective perspective) {
    super(parent, SWT.NONE);
    this.perspective = perspective;
    setLayout(new FillLayout());
    PropsUi.setLook(this);

    scrolledComposite = new ScrolledComposite(this, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    PropsUi.setLook(scrolledComposite);
  }

  /** Rebuilds the overview from the given cards (grouped and ordered by category). */
  public void setCards(List<TypeCard> cards) {
    if (content != null && !content.isDisposed()) {
      content.dispose();
    }

    content = new Composite(scrolledComposite, SWT.NONE);
    GridLayout layout = new GridLayout(1, false);
    layout.marginWidth = 2 * PropsUi.getFormMargin();
    layout.marginHeight = 2 * PropsUi.getFormMargin();
    layout.verticalSpacing = PropsUi.getMargin();
    content.setLayout(layout);
    PropsUi.setLook(content);

    Label title = new Label(content, SWT.NONE);
    title.setText(BaseMessages.getString(PKG, "MetadataPerspective.Overview.Title"));
    title.setFont(GuiResource.getInstance().getFontLarge());
    PropsUi.setLook(title);

    Label subtitle = new Label(content, SWT.WRAP);
    subtitle.setText(BaseMessages.getString(PKG, "MetadataPerspective.Overview.Subtitle"));
    subtitle.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    PropsUi.setLook(subtitle);

    // Group the cards by category id and order the categories the same way the tree does.
    Map<String, List<TypeCard>> cardsByCategory = new LinkedHashMap<>();
    for (TypeCard card : cards) {
      cardsByCategory.computeIfAbsent(card.categoryId(), k -> new ArrayList<>()).add(card);
    }
    List<String> categoryIds = new ArrayList<>(cardsByCategory.keySet());
    categoryIds.sort(
        Comparator.comparingInt(MetadataCategories::orderOf)
            .thenComparing(MetadataCategories::labelFor));

    for (String categoryId : categoryIds) {
      // CLabel (not Label) so the icon and the category name are shown together; a plain SWT Label
      // renders only the image when both an image and text are set.
      CLabel categoryLabel = new CLabel(content, SWT.NONE);
      categoryLabel.setText(MetadataCategories.labelFor(categoryId));
      categoryLabel.setImage(
          GuiResource.getInstance()
              .getImage(
                  MetadataCategories.imageFor(categoryId),
                  getClass().getClassLoader(),
                  ConstUi.SMALL_ICON_SIZE,
                  ConstUi.SMALL_ICON_SIZE));
      categoryLabel.setFont(GuiResource.getInstance().getFontMediumBold());
      GridData categoryGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
      categoryGd.verticalIndent = PropsUi.getMargin();
      categoryLabel.setLayoutData(categoryGd);
      PropsUi.setLook(categoryLabel);

      for (TypeCard card : cardsByCategory.get(categoryId)) {
        createCard(content, card);
      }
    }

    scrolledComposite.setContent(content);
    scrolledComposite.setMinSize(content.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    PropsUi.setTheme(scrolledComposite);

    content.layout();
    scrolledComposite.layout();
  }

  private void createCard(Composite parent, TypeCard card) {
    Composite row = new Composite(parent, SWT.NONE);
    GridLayout rowLayout = new GridLayout(3, false);
    rowLayout.marginWidth = PropsUi.getMargin();
    rowLayout.marginHeight = PropsUi.getMargin();
    row.setLayout(rowLayout);
    GridData rowGd = new GridData(SWT.FILL, SWT.CENTER, true, false);
    rowGd.horizontalIndent = ConstUi.MEDIUM_ICON_SIZE;
    row.setLayoutData(rowGd);
    PropsUi.setLook(row);

    Label icon = new Label(row, SWT.NONE);
    icon.setImage(
        GuiResource.getInstance()
            .getImage(
                card.image(),
                card.imageClassLoader(),
                ConstUi.MEDIUM_ICON_SIZE,
                ConstUi.MEDIUM_ICON_SIZE));
    icon.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, false, false));
    PropsUi.setLook(icon);

    Composite textBlock = new Composite(row, SWT.NONE);
    GridLayout textLayout = new GridLayout(1, false);
    textLayout.marginWidth = 0;
    textLayout.marginHeight = 0;
    textBlock.setLayout(textLayout);
    textBlock.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    PropsUi.setLook(textBlock);

    Label name = new Label(textBlock, SWT.NONE);
    name.setText(card.name() + "  (" + card.count() + ")");
    name.setFont(GuiResource.getInstance().getFontBold());
    name.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    PropsUi.setLook(name);
    // Clicking the name navigates to the type in the tree (when it is visible there).
    name.addListener(SWT.MouseUp, e -> perspective.selectType(card.key()));

    if (card.description() != null && !card.description().isEmpty()) {
      Label description = new Label(textBlock, SWT.WRAP);
      description.setText(card.description());
      description.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
      PropsUi.setLook(description);
    }

    Button newButton = new Button(row, SWT.PUSH);
    newButton.setText(BaseMessages.getString(PKG, "MetadataPerspective.Overview.New.Button"));
    newButton.setLayoutData(new GridData(SWT.RIGHT, SWT.CENTER, false, false));
    newButton.addListener(
        SWT.Selection, e -> perspective.createNewMetadataFromOverview(card.key()));
    PropsUi.setLook(newButton);
  }
}
