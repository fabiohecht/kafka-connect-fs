package ch.generali.copa.di.connector.fssourceconnector.file.reader;

import ch.generali.copa.di.connector.fssourceconnector.FsSourceTaskConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractFileReader<T> implements FileReader {

    private final FileSystem fs;
    private final Path filePath;
    private ReaderAdapter<T> adapter;

    public AbstractFileReader(FileSystem fs, Path filePath, ReaderAdapter adapter, Map<String, Object> config) {
        if (fs == null || filePath == null) {
            throw new IllegalArgumentException("fileSystem and filePath are required");
        }
        this.fs = fs;
        this.filePath = filePath;
        this.adapter = adapter;

        Map<String, Object> readerConf = config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.FILE_READER_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        configure(readerConf);
    }

    protected abstract void configure(Map<String, Object> config);

    protected FileSystem getFs() {
        return fs;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    public final Struct next() {
        return adapter.apply(nextRecord());
    }

    protected abstract T nextRecord();

    protected ReaderAdapter<T> getAdapter() {
        return adapter;
    }

}
