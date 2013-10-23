package com.emar.recsys.user.mahout;

import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.*;
import com.emar.recsys.user.util.mr.HdfsDAO;
import com.google.common.primitives.Longs;

public class ItemCF {
	// TEST: hdfs://host116:9000/recommend/user/zhouliaoming 
	private static final String HDFS = "hdfs://host137:9000/recommend/user/zhouliaoming";

	public static void main(String[] args) throws Exception {
		String localFile = "/dmp4/recommend/user/zhouliaoming/data/item.csv";
		String inPath = HDFS + "/data/userCF";
		String inFile = inPath + "/item.csv";
		String outPath = HDFS + "/test/itemCF_result/";
		String outFile = outPath + "/part-r-00000";
		String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();
		System.out.println("[Info] Test Output.");
		System.out.println("[Info] Longs="+ Longs.hashCode(0));
		System.out.println("[Info] Longs-util=" + TasteHadoopUtils.idToIndex(0));

		JobConf conf = config();
		HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
		hdfs.rmr(inPath);
		hdfs.mkdirs(inPath);
		hdfs.copyFile(localFile, inPath);
		hdfs.ls(inPath);
		hdfs.cat(inFile);

		StringBuilder sb = new StringBuilder();
		sb.append("--input ").append(inPath);
		sb.append(" --output ").append(outPath);
		sb.append(" --booleanData true");
		sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
		sb.append(" --tempDir ").append(tmpPath);
		args = sb.toString().split(" ");

		RecommenderJob job = new RecommenderJob();
		conf.setNumMapTasks(4);
		conf.setNumReduceTasks(4);
		job.setConf(conf);
		job.run(args);

		hdfs.cat(outFile);
	}

	public static JobConf config() {
		JobConf conf = new JobConf(ItemCF.class);
		conf.setJobName("ItemCF");
//		final String HConf = "/home/hadoop/hadoop-1.0.3/conf";
		final String HConf = "/dmp/hadoop/hadoop-1.0.3/conf";
		conf.addResource("classpath:" + HConf + "/core-site.xml");
		conf.addResource("classpath:" + HConf + "/hdfs-site.xml");
		conf.addResource("classpath:" + HConf + "/mapred-site.xml");
		return conf;
	}
}