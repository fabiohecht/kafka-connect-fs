package ch.generali.copa.di.connector.fssourceconnector.file.reader;

import ch.generali.copa.di.connector.fssourceconnector.file.Offset;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Function;

public interface FileReader extends Iterator<Struct>, Closeable {

    Path getFilePath();

    boolean hasNext();

    Struct next();

    void seek(Offset offset);

    Offset currentOffset();
}

@FunctionalInterface
interface ReaderAdapter<T> extends Function<T, Struct> {

}