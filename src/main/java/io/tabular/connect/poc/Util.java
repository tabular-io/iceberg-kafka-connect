// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

public class Util {

  private static final String CATALOG_PROP = "iceberg.catalog";
  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final int CATALOG_PROP_PREFIX_LEN = CATALOG_PROP_PREFIX.length();

  public static Catalog loadCatalog(Map<String, String> props) {
    String catalogImpl = props.get(CATALOG_PROP);
    Map<String, String> catalogProps = new HashMap<>();
    for (Entry<String, String> entry : props.entrySet()) {
      if (entry.getKey().startsWith(CATALOG_PROP_PREFIX)) {
        catalogProps.put(entry.getKey().substring(CATALOG_PROP_PREFIX_LEN), entry.getValue());
      }
    }

    return CatalogUtil.loadCatalog(catalogImpl, "iceberg", catalogProps, new Configuration());
  }
}
