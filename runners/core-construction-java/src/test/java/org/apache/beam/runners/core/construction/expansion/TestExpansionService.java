/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.core.construction.expansion;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.PortableSchemaCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * An {@link org.apache.beam.runners.core.construction.expansion.ExpansionService} useful for tests.
 */
public class TestExpansionService {

  private static final String TEST_COUNT_URN = "pytest:beam:transforms:count";
  private static final String TEST_FILTER_URN = "pytest:beam:transforms:filter_less_than";
  private static final String TEST_SQL_URN = "pytest:beam:transforms:sql";

  /** Registers a single test transformation. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms implements ExpansionService.ExpansionServiceRegistrar {
    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(
          TEST_COUNT_URN, spec -> Count.perElement(),
          TEST_FILTER_URN,
              spec ->
                  Filter.lessThanEq(
                      // TODO(BEAM-6587): Use strings directly rather than longs.
                      (long) spec.getPayload().toStringUtf8().charAt(0)),
          TEST_SQL_URN,
              spec ->
                  PortableRowTransform.of(SqlTransform.query(spec.getPayload().toStringUtf8())));
    }
  }

  /**
   * Wraps a transform that produces Rows and ensures that it uses the portable beam:coder:row:v1
   */
  public static class PortableRowTransform extends PTransform<PInput, PCollection<Row>> {
    private final PTransform<PInput, PCollection<Row>> wrapped;

    public PortableRowTransform(PTransform<PInput, PCollection<Row>> wrapped) {
      this.wrapped = wrapped;
    }

    public static PortableRowTransform of(PTransform<PInput, PCollection<Row>> wrapped) {
      return new PortableRowTransform(wrapped);
    }

    @Override
    public PCollection<Row> expand(PInput input) {
      PCollection<Row> pc = this.wrapped.expand(input);
      pc.setCoder(PortableSchemaCoder.of(pc.getSchema()));
      return pc;
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);
    Server server = ServerBuilder.forPort(port).addService(new ExpansionService()).build();
    server.start();
    server.awaitTermination();
  }
}
