package ch.generali.copa.di.connector.fssourceconnector.file.reader.local;

import ch.generali.copa.di.connector.fssourceconnector.file.Offset;
import ch.generali.copa.di.connector.fssourceconnector.file.reader.AgnosticFileReader;
import ch.generali.copa.di.connector.fssourceconnector.file.reader.FileReaderTestBase;
import ch.generali.copa.di.connector.fssourceconnector.file.reader.SequenceFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SequenceFileReaderTest extends LocalFileReaderTestBase {

    private static final String FIELD_NAME_KEY = "custom_field_key";
    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "sq";

    @BeforeClass
    public static void setUp() throws IOException {
        FileReaderTestBase.readerClass = AgnosticFileReader.class;
        FileReaderTestBase.dataFile = createDataFile();
        FileReaderTestBase.readerConfig = new HashMap<String, Object>() {{
            put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_KEY, FIELD_NAME_KEY);
            put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE, FILE_EXTENSION);
        }};
    }

    private static Path createDataFile() throws IOException {
        File seqFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (SequenceFile.Writer writer = SequenceFile.createWriter(FileReaderTestBase.fs.getConf(), SequenceFile.Writer.file(new Path(seqFile.getAbsolutePath())),
                SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class))) {

            IntStream.range(0, FileReaderTestBase.NUM_RECORDS).forEach(index -> {
                Writable key = new IntWritable(index);
                Writable value = new Text(String.format("%d_%s", index, UUID.randomUUID()));
                try {
                    writer.append(key, value);
                    writer.sync();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        try (SequenceFile.Reader reader = new SequenceFile.Reader(FileReaderTestBase.fs.getConf(),
                SequenceFile.Reader.file(new Path(seqFile.getAbsolutePath())))) {
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), FileReaderTestBase.fs.getConf());
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), FileReaderTestBase.fs.getConf());
            int index = 0;
            long pos = reader.getPosition() - 1;
            while (reader.next(key, value)) {
                FileReaderTestBase.OFFSETS_BY_INDEX.put(index++, pos);
                pos = reader.getPosition();
            }
        }
        Path path = new Path(new Path(FileReaderTestBase.fsUri), seqFile.getName());
        FileReaderTestBase.fs.moveFromLocalFile(new Path(seqFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void defaultFieldNames() throws Throwable {
        Map<String, Object> customReaderCfg = new HashMap<String, Object>() {{
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE, getFileExtension());
        }};
        FileReaderTestBase.reader = getReader(FileReaderTestBase.fs, FileReaderTestBase.dataFile, customReaderCfg);
        assertTrue(FileReaderTestBase.reader.getFilePath().equals(FileReaderTestBase.dataFile));

        assertTrue(FileReaderTestBase.reader.hasNext());

        int recordCount = 0;
        while (FileReaderTestBase.reader.hasNext()) {
            Struct record = FileReaderTestBase.reader.next();
            checkData(SequenceFileReader.FIELD_NAME_KEY_DEFAULT, SequenceFileReader.FIELD_NAME_VALUE_DEFAULT, record, recordCount);
            recordCount++;
        }
        Assert.assertEquals("The number of records in the file does not match", FileReaderTestBase.NUM_RECORDS, recordCount);
    }

    @Override
    protected Offset getOffset(long offset) {
        return new SequenceFileReader.SeqOffset(offset);
    }

    @Override
    protected void checkData(Struct record, long index) {
        checkData(FIELD_NAME_KEY, FIELD_NAME_VALUE, record, index);
    }

    private void checkData(String keyFieldName, String valueFieldName, Struct record, long index) {
        assertTrue((Integer) record.get(keyFieldName) == index);
        assertTrue(record.get(valueFieldName).toString().startsWith(index + "_"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
