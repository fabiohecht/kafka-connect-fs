package ch.generali.copa.di.connector.fssourceconnector.policy;

import ch.generali.copa.di.connector.fssourceconnector.FsSourceTaskConfig;

import java.io.IOException;
import java.util.Map;

public class SimplePolicy extends AbstractPolicy {

    public SimplePolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {

    }

    @Override
    protected boolean isPolicyCompleted() {
        return getExecutions() > 0;
    }

}
