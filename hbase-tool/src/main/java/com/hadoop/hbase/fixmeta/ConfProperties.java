package com.asiainfo.hbase.fixmeta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfProperties {

	private static Properties conf;
	private static String path;

	public static Properties getConf() {
		if (conf == null) {
			synchronized (ConfProperties.class) {
				if (conf == null) {
					conf = new Properties();
					new ConfProperties(path);
				}
			}
		}
		return conf;
	}

	public static void setPath(String path){
		ConfProperties.path = path;
	}
	
	
	private ConfProperties(String path) {
		loadConf(path);
	}

	private void loadConf(String path) {
		InputStream inputStream = null;
		try {
			String confpath = path + File.separator + "configuration.properties";
			inputStream = new FileInputStream(new File(confpath));
			conf.load(inputStream);
		} catch (Exception e) {
			System.out.println("Exception while loadConf(): " + e);
			throw new RuntimeException(e);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					System.out.println("IOException while loadConf(): " + e);
				}
			}
		}
	}

}
