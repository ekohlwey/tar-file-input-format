package com.edkohlwey.hadoop;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TarGzRecordReader extends RecordReader<Text, BytesWritable> {
    private byte[] readBuffer = new byte[5 * 2 ^ 20]; // 5mb default buffer
    private TarArchiveInputStream archiveInputStream;
    private final BytesWritable currentContents = new BytesWritable();
    private final Text currentArchiveAndFile = new Text();
    private String currentArchive;
    private long fileLength;
    private FSDataInputStream fsInputStream;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        fileLength = fs.getFileStatus(path).getLen();
        currentArchive = path.toString();
        fsInputStream = fs.open(path);
        archiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(fsInputStream));
    }

    private static boolean isNotFileEntryOrNull(TarArchiveEntry entry) {
        return entry != null && !entry.isFile();
    }

    private TarArchiveEntry skipToNextFileEntry() throws IOException {
        TarArchiveEntry entry = archiveInputStream.getNextTarEntry();
        while (isNotFileEntryOrNull(entry)) {
            entry = archiveInputStream.getNextTarEntry();
        }
        return entry;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        TarArchiveEntry entry = skipToNextFileEntry();
        if (entry == null) {
            return false;
        }
        currentArchiveAndFile.set(currentArchive + "#" + entry.getName());
        long entrySize = entry.getRealSize();
        if (entrySize > Integer.MAX_VALUE) {
            throw new IOException("File larger than int.MAX_VALUE");
        }
        int intEntrySize = (int) entrySize;
        if (intEntrySize > readBuffer.length) {
            readBuffer = new byte[intEntrySize];
        }
        archiveInputStream.read(readBuffer, 0, intEntrySize);
        currentContents.set(readBuffer, 0, intEntrySize);
        return true;
    }


    @Override
    public Text getCurrentKey() {
        return currentArchiveAndFile;
    }

    @Override
    public BytesWritable getCurrentValue() {
        return currentContents;
    }

    @Override
    public float getProgress() throws IOException {
        return ((float) fsInputStream.getPos()) / fileLength;
    }

    @Override
    public void close() throws IOException {
        archiveInputStream.close();
    }
}
