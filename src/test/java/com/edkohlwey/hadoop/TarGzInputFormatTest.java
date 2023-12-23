package com.edkohlwey.hadoop;


import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

public class TarGzInputFormatTest {

    @TempDir
    static Path tempDir;
    static Configuration conf = new Configuration();
    static MiniMRClientCluster testCluster;

    @BeforeAll
    static public void checJavaHome() {
        String javaHome = System.getenv("JAVA_HOME");
        Assertions.assertNotNull(javaHome, "JAVA_HOME must be set in order to run this test.");
    }

    @BeforeAll
    static public void startCluster() throws Exception {
        testCluster = MiniMRClientClusterFactory.create(TarGzInputFormatTest.class, 1, conf);
        testCluster.start();
    }

    @AfterAll
    static public void stopCluster() throws Exception {
        testCluster.stop();
    }


    @Test
    public void canReadData() throws Throwable {
        Path testFile = tempDir.resolve("test.tar.gz");
        byte[] fileBytes = "test info!\n".getBytes(StandardCharsets.UTF_8);
        try (OutputStream fileOutputStream = Files.newOutputStream(testFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
             GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(bufferedOutputStream);
             TarArchiveOutputStream tarfileOutput = new TarArchiveOutputStream(gzipOutputStream)) {
            TarArchiveEntry tarEntry = new TarArchiveEntry("file.txt");
            tarEntry.setMode(TarArchiveEntry.DEFAULT_FILE_MODE);
            tarEntry.setSize(fileBytes.length);
            tarfileOutput.putArchiveEntry(tarEntry);
            tarfileOutput.write(fileBytes);
            tarfileOutput.closeArchiveEntry();
            tarfileOutput.finish();
        }
        Configuration conf = testCluster.getConfig();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TarGzInputFormatTest.class);
        job.setInputFormatClass(TarGzInputFormat.class);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(testFile.toUri()));
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setReducerClass(Reducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputDir = tempDir.resolve("output");
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(outputDir.toUri()));
        job.submit();
        job.waitForCompletion(true);
        Path successFilePath = outputDir.resolve("_SUCCESS");
        Assertions.assertTrue(Files.exists(successFilePath), "The _SUCCESS file should exist after the job " +
                "run");
        Path outputFile = outputDir.resolve("part-r-00000");
        Assertions.assertTrue(Files.exists(outputFile), "The output file part-r-00000 should exist");
        String output = Files.readString(outputFile);
        BytesWritable expectedOutputFileBytes = new BytesWritable(fileBytes);

        assertThat(output, containsString(testFile.toString()));
        assertThat(output, containsString(expectedOutputFileBytes.toString()));
        assertThat(output, containsString("file.txt"));

    }

}
