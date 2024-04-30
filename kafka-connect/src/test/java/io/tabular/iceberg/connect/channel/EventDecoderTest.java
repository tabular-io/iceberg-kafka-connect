package io.tabular.iceberg.connect.channel;

import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class EventDecoderTest {

    private byte[] event(String file) {
        try {
            URL jsonResource = this.getClass().getClassLoader().getResource(file);
            return Files.readAllBytes(Paths.get(jsonResource.toURI()));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not load %s for test", file));
        }
    }

    @Test
    public void testCommitRequestBecomesStartCommit() {
        Event result = EventDecoder.decode(event("commit_request.event"));
        assertThat(result.type()).isEqualTo(PayloadType.START_COMMIT);
        assertThat(result.payload()).isInstanceOf(StartCommit.class);
        StartCommit payload = (StartCommit) result.payload();
        // hardcoded in the commit_request.event file
        assertThat(payload.commitId()).isEqualTo(UUID.fromString("389c1c97-c3a4-4f0a-be9c-60ba49d490ff"));
    }
}
