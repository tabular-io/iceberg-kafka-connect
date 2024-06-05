/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.transforms;

import io.tabular.iceberg.connect.exception.DeadLetterUtils;
import io.tabular.iceberg.connect.exception.FailedRecordFactory;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class DefaultExceptionHandler implements TransformExceptionHandler {

  private static final String FAILED_RECORD_FACTORY_PROP = "failed_record_factory";

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              FAILED_RECORD_FACTORY_PROP,
              ConfigDef.Type.STRING,
              "io.tabular.iceberg.connect.exception.DefaultFailedRecordFactory",
              ConfigDef.Importance.MEDIUM,
              "class name for failed record conversion");

  private FailedRecordFactory recordFactory;

  @Override
  public SinkRecord handle(SinkRecord original, Throwable error) {
    return recordFactory.recordFromSmt(original, error);
  }

  @Override
  public void configure(Map<String, String> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    ClassLoader loader = this.getClass().getClassLoader();
    this.recordFactory =
        (FailedRecordFactory)
            DeadLetterUtils.loadClass(config.getString(FAILED_RECORD_FACTORY_PROP), loader);
    recordFactory.configure(props);
  }
}
