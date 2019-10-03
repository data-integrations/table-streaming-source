/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.table.streaming;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableStreamingSourceConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema SCHEMA = Schema
    .recordOf("simpleTableSchema",
              Schema.Field.of("string_value", Schema.of(Schema.Type.STRING)),
              Schema.Field.of("int_value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
              Schema.Field.of("float_value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
              Schema.Field.of("boolean_value", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))
    );
  private static final TableStreamingSourceConfig VALID_CONFIG = new TableStreamingSourceConfig(
    "test",
    SCHEMA.toString(),
    null,
    null
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateCorrectRowField() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    TableStreamingSourceConfig config = TableStreamingSourceConfig.builder(VALID_CONFIG)
      .setRowField("string_value")
      .build();

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateCorrectRefreshInterval() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    TableStreamingSourceConfig config = TableStreamingSourceConfig.builder(VALID_CONFIG)
      .setRefreshInterval("5d")
      .build();

    config.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testValidateIncorrectRowField() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    TableStreamingSourceConfig config = TableStreamingSourceConfig.builder(VALID_CONFIG)
      .setRowField("test")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(TableStreamingSourceConfig.ROW_FIELD)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateRowFieldNullableField() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    TableStreamingSourceConfig config = TableStreamingSourceConfig.builder(VALID_CONFIG)
      .setRowField("int_value")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(TableStreamingSourceConfig.ROW_FIELD)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateIncorrectRefreshInterval() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    TableStreamingSourceConfig config = TableStreamingSourceConfig.builder(VALID_CONFIG)
      .setRefreshInterval("t3")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(TableStreamingSourceConfig.REFRESH_INTERVAL)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  @Test
  public void testValidateIncorrectSchema() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    TableStreamingSourceConfig config = TableStreamingSourceConfig.builder(VALID_CONFIG)
      .setSchema("test")
      .build();
    List<List<String>> paramNames = Collections.singletonList(
      Collections.singletonList(TableStreamingSourceConfig.SCHEMA)
    );

    config.validate(failureCollector);
    assertValidationFailed(failureCollector, paramNames);
  }

  private void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
