// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.convert;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;

public interface RecordConverter {

  Record convert(Object value, StructType tableSchema);
}
