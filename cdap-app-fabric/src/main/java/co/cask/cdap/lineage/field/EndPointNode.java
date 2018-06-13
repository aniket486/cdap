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

package co.cask.cdap.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;

import java.util.Objects;

/**
 * Node in the {@link FieldLineageGraph} representing EndPoint.
 */
public class EndPointNode extends Node {
  private final EndPoint endPoint;

  EndPointNode(EndPoint endPoint) {
    super(Type.ENDPOINT);
    this.endPoint = endPoint;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    EndPointNode that = (EndPointNode) o;
    return Objects.equals(endPoint, that.endPoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), endPoint);
  }
}
