package ch.generali.copa.di.connector.fssourceconnector.task;

import ch.generali.copa.di.connector.fssourceconnector.FsSourceConnectorConfig;
import ch.generali.copa.di.connector.fssourceconnector.FsSourceTaskConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class FsSourceTaskConfigTest {

    @Test
    public void checkDocumentation() {
        ConfigDef config = FsSourceTaskConfig.conf();
        config.names().forEach(key -> {
            assertFalse("Property " + key + " should be documented",
                    config.configKeys().get(key).documentation == null ||
                            "".equals(config.configKeys().get(key).documentation.trim()));
        });
    }

    @Test
    public void toRst() {
        assertNotNull(FsSourceConnectorConfig.conf().toRst());
    }
}