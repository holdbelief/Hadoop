package com.sxt.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHdfsApi {

	Configuration conf = null;
	FileSystem fs = null;

	@Before
	public void conn() throws IOException {
		conf = new Configuration(true);
		fs = FileSystem.get(conf);
	}

	@Test
	public void mkdir() throws IOException {
		Path tempDir = new Path("/temp");
		if (!fs.exists(tempDir)) {
			fs.mkdirs(tempDir);
		}
	}
	
	@Test
	public void uploadFile() throws IOException {
		Path sxtFile = new Path("/temp/sxt.txt");
		FSDataOutputStream output = fs.create(sxtFile);
		InputStream input = new BufferedInputStream(new FileInputStream(new File("/home/faith/.xsession-errors")));
	
		IOUtils.copyBytes(input, output, conf, true);
	}

	@After
	public void close() throws IOException {
		fs.close();
	}
}
