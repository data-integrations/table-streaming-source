/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATASTREAMS_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-streams", "3.2.0");
  protected static final ArtifactSummary DATASTREAMS_ARTIFACT = new ArtifactSummary("data-streams", "3.2.0");

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupTest() throws Exception {
    setupStreamingArtifacts(DATASTREAMS_ARTIFACT_ID, DataStreamsApp.class);

    // add artifact for plugin
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), DATASTREAMS_ARTIFACT_ID,
                      TableStreamingSource.class);
  }

  @Test
  public void testTableStreamingSource() throws Exception {
    Schema schema = Schema.recordOf("item",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    Map<String, String> properties = ImmutableMap.of(
      "name", "taybull",
      "schema", schema.toString(),
      "rowField", "id",
      "refreshInterval", "5s"
    );

    DataStreamsConfig pipelineConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source",
                             new ETLPlugin("Table", StreamingSource.PLUGIN_TYPE, properties, null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("tableOutput")))
      .addConnection("source", "sink")
      .setBatchInterval("1s")
      .build();
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, pipelineConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("RefreshableTableApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset("taybull");
    Put put = new Put(Bytes.toBytes(1L));
    put.add("name", "Samuel");
    inputManager.get().put(put);
    inputManager.flush();

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    final DataSetManager<Table> outputManager = getDataset("tableOutput");
    final Map<Long, String> expected = new HashMap<>();
    expected.put(1L, "Samuel");
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> contents = new HashMap<>();
          for (StructuredRecord record : MockSink.readOutput(outputManager)) {
            contents.put((Long) record.get("id"), (String) record.get("name"));
          }
          return expected.equals(contents);
        }
      },
      2,
      TimeUnit.MINUTES);

    put = new Put(Bytes.toBytes(2L));
    put.add("name", "L");
    inputManager.get().put(put);
    inputManager.flush();

    expected.put(2L, "L");
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> contents = new HashMap<>();
          for (StructuredRecord record : MockSink.readOutput(outputManager)) {
            contents.put((Long) record.get("id"), (String) record.get("name"));
          }
          return expected.equals(contents);
        }
      },
      2,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 10, 1);

    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    put = new Put(Bytes.toBytes(3L));
    put.add("name", "Jackson");
    inputManager.get().put(put);
    inputManager.flush();

    expected.put(3L, "Jackson");
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Map<Long, String> contents = new HashMap<>();
          for (StructuredRecord record : MockSink.readOutput(outputManager)) {
            contents.put((Long) record.get("id"), (String) record.get("name"));
          }
          return expected.equals(contents);
        }
      },
      2,
      TimeUnit.MINUTES);
  }

}
