/*
 *
 * Copyright © 2017-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.RowRecordTransformer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.reflect.ClassTag$;

/**
 * A StreamingSource that returns the entire contents of a Table as each micro batch and refreshes the contents
 * after some configurable amount of time
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Table")
@Description("Returns the entire contents of a Table each batch interval, refreshing the contents at configurable " +
  "intervals. The primary use case for this plugin is to send it to a Joiner plugin to provide lookup-like " +
  "functionality.")
public class TableStreamingSource extends StreamingSource<StructuredRecord> {
  private final TableStreamingSourceConfig config;

  public TableStreamingSource(TableStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    Schema schema = config.getSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    if (!config.containsMacro("name")) {
      pipelineConfigurer.createDataset(config.getName(), Table.class.getName(), getTableProperties(schema));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    FailureCollector failureCollector = streamingContext.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    JavaSparkExecutionContext cdapContext = streamingContext.getSparkExecutionContext();
    Admin admin = cdapContext.getAdmin();
    final Schema schema = config.getSchema();
    if (!admin.datasetExists(config.getName())) {
      try {
        admin.createDataset(config.getName(), "table", getTableProperties(schema));
      } catch (InstanceConflictException e) {
        // this is ok, means it was created after we checked that it didn't exist but before we were able to create it
      }
    }
    streamingContext.registerLineage(config.getName());

    JavaStreamingContext jsc = streamingContext.getSparkStreamingContext();
    return JavaDStream.fromDStream(
      new TableInputDStream(cdapContext, jsc.ssc(), config.getName(),
                            config.getRefreshInterval(), 0L, null),
      ClassTag$.MODULE$.apply(Tuple2.class))
      .map(new RowToRecordFunc(schema, config.getRowField()));
  }

  /**
   * Transforms a row to a record.
   */
  private static class RowToRecordFunc implements Function<Tuple2<byte[], Row>, StructuredRecord> {
    private final Schema schema;
    private final String rowField;
    private RowRecordTransformer rowRecordTransformer;

    private RowToRecordFunc(Schema schema, String rowField) {
      this.schema = schema;
      this.rowField = rowField;
    }

    @Override
    public StructuredRecord call(Tuple2<byte[], Row> row) {
      if (rowRecordTransformer == null) {
        rowRecordTransformer = new RowRecordTransformer(schema, rowField);
      }
      return rowRecordTransformer.toRecord(row._2());
    }
  }

  private DatasetProperties getTableProperties(Schema schema) {
    TableProperties.Builder tableProperties = TableProperties.builder().setSchema(schema);
    if (config.getRowField() != null) {
      tableProperties.setRowFieldName(config.getRowField());
    }
    return tableProperties.build();
  }

}
