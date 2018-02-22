package ch.generali.copa.di.connector.fssourceconnector.policy.hdfs;

import ch.generali.copa.di.connector.fssourceconnector.file.reader.TextFileReader;
import ch.generali.copa.di.connector.fssourceconnector.policy.PolicyTestBase;
import ch.generali.copa.di.connector.fssourceconnector.policy.SimplePolicy;
import ch.generali.copa.di.connector.fssourceconnector.FsSourceTaskConfig;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SimplePolicyTest extends HdfsPolicyTestBase {

    @BeforeClass
    public static void setUp() throws IOException {
        PolicyTestBase.directories = new ArrayList<Path>() {{
            add(new Path(PolicyTestBase.fsUri.toString(), UUID.randomUUID().toString()));
            add(new Path(PolicyTestBase.fsUri.toString(), UUID.randomUUID().toString()));
        }};
        for (Path dir : PolicyTestBase.directories) {
            PolicyTestBase.fs.mkdirs(dir);
        }

        Map<String, String> cfg = new HashMap<String, String>() {{
            String uris[] = PolicyTestBase.directories.stream().map(dir -> dir.toString())
                    .toArray(size -> new String[size]);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "test");
        }};
        PolicyTestBase.taskConfig = new FsSourceTaskConfig(cfg);
    }
}
