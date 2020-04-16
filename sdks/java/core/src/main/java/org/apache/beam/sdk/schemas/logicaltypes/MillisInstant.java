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
package org.apache.beam.sdk.schemas.logicaltypes;

import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** A timestamp represented as nanoseconds since the epoch. */
public class MillisInstant extends MillisType<ReadableInstant> {
  public static final String IDENTIFIER = "beam:logical_type:millis_instant:v1";

  private MillisInstant() {
    super(IDENTIFIER);
  }

  public static MillisInstant of() {
    return new MillisInstant();
  }

  @Override
  public Long toBaseType(ReadableInstant input) {
    return input.getMillis();
  }

  @Override
  public ReadableInstant toInputType(Long millis) {
    return Instant.ofEpochMilli(millis);
  }
}
