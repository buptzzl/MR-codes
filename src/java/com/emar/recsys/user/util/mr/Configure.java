package com.emar.recsys.user.util.mr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 对MR 的配置的操作。 与通用的测试程序。
 * 
 * @author zhoulm
 * 
 */
public class Configure extends Configured implements Tool {

	static {
//		Configuration.addDefaultResource("hdfs-default.xml");
//		Configuration.addDefaultResource("hdfs-site.xml");
//		Configuration.addDefaultResource("mapred-default.xml");
//		Configuration.addDefaultResource("mapred-site.xml");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] oargs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Configuration myconf = new Configuration();
		myconf.addResource("D:/Data/MR-codes/conf/hadoop-add.xml");
	
		System.out.println("[info] myconf=" + myconf.get("fs.default.name") 
				+ " color2=" + myconf.get("color2"));
		
		List<String> carr = new ArrayList<String>();
		for (Entry<String, String> entry : conf) {
//			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
			carr.add("\n" + entry.toString());
		}
		Collections.sort(carr);
		System.out.println("[Info] res=" + carr);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configure(), args);
		System.exit(exitCode);
	}

}
