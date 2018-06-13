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

import java.util.Objects;

/**
 * Represent single connection between source {@link Node} and destination {@link Node}
 * in the {@link FieldLineageGraph}. Edge does not carry additional information.
 */
public class Edge {
  private final Node source;
  private final Node destination;

  Edge(Node source, Node destination) {
    this.source = source;
    this.destination = destination;
  }

  Node getSource() {
    return source;
  }

  Node getDestination() {
    return destination;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Edge edge = (Edge) o;
    return Objects.equals(source, edge.source) &&
            Objects.equals(destination, edge.destination);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, destination);
  }
}
