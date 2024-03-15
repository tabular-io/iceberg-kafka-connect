package io.tabular.iceberg.connect.transforms;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class FileLoads {
    protected final String getFile(String fileName) throws IOException, URISyntaxException {
        URL jsonResource = getClass().getClassLoader().getResource(fileName);
        return new String(Files.readAllBytes(Paths.get(jsonResource.toURI())), StandardCharsets.UTF_8);
    }
}
