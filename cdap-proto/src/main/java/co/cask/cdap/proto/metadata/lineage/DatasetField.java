/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.proto.metadata.lineage;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.proto.id.DatasetId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a record in a {@link FieldLineageSummary}. Each record consists
 * of dataset and subset of its fields. The combination of both dataset and field
 * can represent either the origin or destination in the field lineage summary for a
 * given field.
 */
@Beta
public class DatasetField {
  private final DatasetId dataset;
  private final List<String> fields;

  public DatasetField(DatasetId dataset, List<String> fields) {
    this.dataset = dataset;
    this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
  }

  public DatasetId getDataset() {
    return dataset;
  }

  public List<String> getFields() {
    return fields;
  }
}
