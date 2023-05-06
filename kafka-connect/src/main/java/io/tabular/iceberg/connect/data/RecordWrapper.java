// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import java.util.Map;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;

public class RecordWrapper implements Record {

  private final Record delegate;
  private final Operation op;

  public RecordWrapper(Record delegate, Operation op) {
    this.delegate = delegate;
    this.op = op;
  }

  public Operation op() {
    return op;
  }

  @Override
  public StructType struct() {
    return delegate.struct();
  }

  @Override
  public Object getField(String name) {
    return delegate.getField(name);
  }

  @Override
  public void setField(String name, Object value) {
    delegate.setField(name, value);
  }

  @Override
  public Object get(int pos) {
    return delegate.get(pos);
  }

  @Override
  public Record copy() {
    return new RecordWrapper(delegate.copy(), op);
  }

  @Override
  public Record copy(Map<String, Object> overwriteValues) {
    return new RecordWrapper(delegate.copy(overwriteValues), op);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return delegate.get(pos, javaClass);
  }

  @Override
  public <T> void set(int pos, T value) {
    delegate.set(pos, value);
  }
}
