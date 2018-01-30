package com.sxt.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	@After
	public void close() throws IOException {
		fs.close();
	}
}
