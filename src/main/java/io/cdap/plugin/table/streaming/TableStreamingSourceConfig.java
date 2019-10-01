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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.TimeParser;

import java.io.IOException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Config for the plugin
 */
public class TableStreamingSourceConfig extends PluginConfig {
  public static final String NAME = "name";
  public static final String SCHEMA = "schema";
  public static final String ROW_FIELD = "rowField";
  public static final String REFRESH_INTERVAL = "refreshInterval";

  @Name(NAME)
  @Macro
  @Description("The name of the CDAP Table to read from.")
  private String name;

  @Name(SCHEMA)
  @Description("The schema to use when reading from the table. If the table does not already exist, one will be " +
    "created with this schema, which will allow the table to be explored through CDAP.")
  @Nullable
  private String schema;

  @Name(ROW_FIELD)
  @Description("Optional schema field whose value is derived from the Table row instead of from a Table column. " +
    "The field name specified must be present in the schema, and must not be nullable.")
  @Nullable
  private String rowField;

  @Name(REFRESH_INTERVAL)
  @Description("How often the table contents should be refreshed. Must be specified by a number followed by " +
    "a unit where 's', 'm', 'h', and 'd' are valid units corresponding to seconds, minutes, hours, and days. " +
    "Defaults to '1h'.")
  @Nullable
  private String refreshInterval;

  public TableStreamingSourceConfig(String name, @Nullable String schema, @Nullable String rowField,
                                    @Nullable String refreshInterval) {
    this.name = name;
    this.schema = schema;
    this.rowField = rowField;
    this.refreshInterval = refreshInterval;
  }

  private TableStreamingSourceConfig(Builder builder) {
    this.name = builder.name;
    this.schema = builder.schema;
    this.rowField = builder.rowField;
    this.refreshInterval = builder.refreshInterval;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(TableStreamingSourceConfig copy) {
    return builder()
      .setName(copy.name)
      .setSchema(copy.schema)
      .setRowField(copy.rowField)
      .setRefreshInterval(copy.refreshInterval);
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getRowField() {
    return rowField;
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse the schema. Reason: " + e.getMessage());
    }
  }

  public long getRefreshInterval() {
    return refreshInterval == null ? TimeParser.parseDuration("1h") : TimeParser.parseDuration(refreshInterval);
  }

  public void validate(FailureCollector failureCollector) {
    if (!Strings.isNullOrEmpty(refreshInterval)) {
      if (!Pattern.matches("\\d+[dhms]{1}$", refreshInterval)) {
        failureCollector.addFailure(
          String.format("Incorrect format of refresh Interval: ", refreshInterval),
          "Refresh Interval must be in format: number followed by one of the unit 's', 'm', 'h', and 'd'")
          .withConfigProperty(REFRESH_INTERVAL);
      }
    }
    try {
      Schema.parseJson(schema);
    } catch (IOException e) {
      failureCollector.addFailure("Unable to parse the schema.", null)
        .withStacktrace(e.getStackTrace())
        .withConfigProperty(SCHEMA);
      return;
    }
    Schema schema = getSchema();
    if (!Strings.isNullOrEmpty(rowField)) {
      if (schema.getField(rowField) == null) {
        failureCollector.addFailure(String.format("Row field '%s' must be present in the schema", getRowField()), null)
          .withConfigProperty(ROW_FIELD)
          .withInputSchemaField(getRowField());
        return;
      }
      if(schema.getField(rowField).getSchema().isNullable()) {
        failureCollector.addFailure(String.format("Row field '%s' must be not nullable.", getRowField()), null)
          .withConfigProperty(ROW_FIELD)
          .withInputSchemaField(getRowField());
      }
    }
  }

  /**
   * Builder for TableStreamingSourceConfig
   */
  public static final class Builder {
    private String name;
    private String schema;
    private String rowField;
    private String refreshInterval;

    private Builder() {
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder setRowField(String rowField) {
      this.rowField = rowField;
      return this;
    }

    public Builder setRefreshInterval(String refreshInterval) {
      this.refreshInterval = refreshInterval;
      return this;
    }

    public TableStreamingSourceConfig build() {
      return new TableStreamingSourceConfig(this);
    }
  }
}
