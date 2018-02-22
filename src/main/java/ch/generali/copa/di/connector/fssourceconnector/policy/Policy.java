package ch.generali.copa.di.connector.fssourceconnector.policy;

import ch.generali.copa.di.connector.fssourceconnector.file.FileMetadata;
import ch.generali.copa.di.connector.fssourceconnector.file.reader.FileReader;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public interface Policy extends Closeable {

    Iterator<FileMetadata> execute() throws IOException;

    FileReader offer(FileMetadata metadata, OffsetStorageReader offsetStorageReader) throws IOException;

    boolean hasEnded();

    List<String> getURIs();

    void interrupt();
}
