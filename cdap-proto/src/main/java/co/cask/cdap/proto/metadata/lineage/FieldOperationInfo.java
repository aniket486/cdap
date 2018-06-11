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

/**
 * Represents field lineage operation. Each individual operation
 * consists of name, description along with the {@link FieldOperationInput}s
 * it operate on and {@link FieldOperationOutput}s it creates.
 */
@Beta
public class FieldOperationInfo {
  private final String name;
  private final String description;
  private final FieldOperationInput inputs;
  private final FieldOperationOutput outputs;

  public FieldOperationInfo(String name, String description, FieldOperationInput inputs,
                            FieldOperationOutput outputs) {
    this.name = name;
    this.description = description;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public FieldOperationInput getInputs() {
    return inputs;
  }

  public FieldOperationOutput getOutputs() {
    return outputs;
  }
}
