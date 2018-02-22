package ch.generali.copa.di.connector.fssourceconnector.policy.local;

import ch.generali.copa.di.connector.fssourceconnector.FsSourceTaskConfig;
import ch.generali.copa.di.connector.fssourceconnector.file.reader.TextFileReader;
import ch.generali.copa.di.connector.fssourceconnector.policy.Policy;
import ch.generali.copa.di.connector.fssourceconnector.policy.PolicyTestBase;
import ch.generali.copa.di.connector.fssourceconnector.policy.SleepyPolicy;
import ch.generali.copa.di.connector.fssourceconnector.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SleepyPolicyTest extends LocalPolicyTestBase {

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
            put(FsSourceTaskConfig.POLICY_CLASS, SleepyPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "test");
            put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "100");
            put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "1");
        }};
        PolicyTestBase.taskConfig = new FsSourceTaskConfig(cfg);
    }

    @Test(expected = ConfigException.class)
    public void invalidSleepTime() throws Throwable {
        Map<String, String> originals = PolicyTestBase.taskConfig.originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        ReflectionUtils.makePolicy((Class<? extends Policy>) PolicyTestBase.taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
    }

    @Test(expected = ConfigException.class)
    public void invalidMaxExecs() throws Throwable {
        Map<String, String> originals = PolicyTestBase.taskConfig.originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        ReflectionUtils.makePolicy((Class<? extends Policy>) PolicyTestBase.taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
    }

    @Test(expected = ConfigException.class)
    public void invalidSleepFraction() throws Throwable {
        Map<String, String> originals = PolicyTestBase.taskConfig.originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_FRACTION, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        ReflectionUtils.makePolicy((Class<? extends Policy>) PolicyTestBase.taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
    }

    @Test
    public void sleepExecution() throws Throwable {
        Map<String, String> tConfig = PolicyTestBase.taskConfig.originalsStrings();
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "1000");
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "2");
        FsSourceTaskConfig sleepConfig = new FsSourceTaskConfig(tConfig);

        PolicyTestBase.policy = ReflectionUtils.makePolicy((Class<? extends Policy>) PolicyTestBase.taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS),
                sleepConfig);
        assertFalse(PolicyTestBase.policy.hasEnded());
        PolicyTestBase.policy.execute();
        assertFalse(PolicyTestBase.policy.hasEnded());
        PolicyTestBase.policy.execute();
        assertTrue(PolicyTestBase.policy.hasEnded());
    }

    @Test
    public void defaultExecutions() throws Throwable {
        Map<String, String> tConfig = PolicyTestBase.taskConfig.originalsStrings();
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "1");
        tConfig.remove(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS);
        FsSourceTaskConfig sleepConfig = new FsSourceTaskConfig(tConfig);

        PolicyTestBase.policy = ReflectionUtils.makePolicy((Class<? extends Policy>) PolicyTestBase.taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS),
                sleepConfig);

        //it never ends
        for (int i = 0; i < 100; i++) {
            assertFalse(PolicyTestBase.policy.hasEnded());
            PolicyTestBase.policy.execute();
        }
        PolicyTestBase.policy.interrupt();
        assertTrue(PolicyTestBase.policy.hasEnded());
    }
}
