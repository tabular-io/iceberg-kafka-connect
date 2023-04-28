// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import java.io.Serializable;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.StructLike;

public interface Element extends StructLike, IndexedRecord, SchemaConstructable, Serializable {}
