import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ParallelMitosis {

	public static class PointMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<_DataPoint> inputData = new ArrayList<_DataPoint>();

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				inputData.add(new _DataPoint(word.toString(), 0));
			}
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			FileInputStream fs = new FileInputStream(new File(
					cacheFiles[0].toString()));

			DataInputStream in = new DataInputStream(fs);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String l;
			while ((l = br.readLine()) != null) {
				_DataPoint current = new _DataPoint(l, 0);
				for (int i = 0; i < inputData.size(); i++) {
					_DataPoint processed = inputData.get(i);
					double currentDistance = current
							.calculateDistance(processed);
					processed.setMinDistance(currentDistance);

				}
			}
			br.close();
			in.close();
			fs.close();

			fs = new FileInputStream(new File(cacheFiles[0].toString()));
			in = new DataInputStream(fs);
			br = new BufferedReader(new InputStreamReader(in));

			while ((l = br.readLine()) != null) {
				_DataPoint current = new _DataPoint(l, 0);
				for (int i = 0; i < inputData.size(); i++) {
					_DataPoint processed = inputData.get(i);
					double currentDistance = current
							.calculateDistance(processed);
					if (currentDistance <= (Global.F * processed
							.getMinDistance())) {
						processed.incrementNeighbors();
						processed.addDistance(currentDistance);

					}
				}
			}
			br.close();
			in.close();
			fs.close();

			for (int i = 0; i < inputData.size(); i++) {
				_DataPoint processed = inputData.get(i);
				context.write(new Text(processed.toStringTwo()), one);
			}

		}

	}

	public static class AssociationMapper extends
			Mapper<Object, Text, _Association, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<_DataPoint> inputData = new ArrayList<_DataPoint>();

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				inputData.add(new _DataPoint(word.toString(), 2));
			}

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			File f = new File(cacheFiles[1].toString() + "/part-r-00000");

			FileInputStream fs = new FileInputStream(f);
			DataInputStream in = new DataInputStream(fs);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String l;
			while ((l = br.readLine()) != null) {
				_DataPoint current = new _DataPoint(l, 2);
				for (int i = 0; i < inputData.size(); i++) {
					_DataPoint processed = inputData.get(i);
					double currentDistance = current
							.calculateDistance(processed);
					if (currentDistance <= (Global.F * (processed
							.getMinDistance()))) {
						context.write(new _Association(processed, current,
								currentDistance), one);

					}
				}
			}
			br.close();
			in.close();
			fs.close();
		}
	}

	public static class AssociationCombiner extends
			Reducer<_Association, IntWritable, _Association, IntWritable> {
		private static IntWritable one = new IntWritable(1);

		@Override
		public void reduce(_Association key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, one);
		}
	}

	public static class PointsCombiner extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable one = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			context.write(key, one);
		}
	}

	public static class PointsReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			context.write(key, one);
		}
	}

	public static class AssociationReducer extends
			Reducer<_Association, IntWritable, Text, IntWritable> {

		private static IntWritable one = new IntWritable(1);

		@Override
		public void reduce(_Association key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(new Text(key.toString()), one);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.exit(2);
		}
		DistributedCache.addCacheFile(new URI("/user/hduser/t4.8k.dat"), conf);
		Job job = new Job(conf, "phase one a");
		job.setJarByClass(ParallelMitosis.class);
		job.setInputFormatClass(NLinesInputFormat.class);
		job.setMapperClass(PointMapper.class);
		job.setCombinerClass(PointsCombiner.class);
		job.setReducerClass(PointsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(Global.NUMOFREDUCERS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);

		DistributedCache.addCacheFile(new URI(args[1]), conf);

		Job job2 = new Job(conf, "phase one b");
		job2.setJarByClass(ParallelMitosis.class);
		job2.setInputFormatClass(NLinesInputFormat.class);
		job2.setMapperClass(AssociationMapper.class);
		job2.setMapOutputKeyClass(_Association.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setCombinerClass(AssociationCombiner.class);
		job2.setReducerClass(AssociationReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(Global.NUMOFREDUCERS);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		DistributedCache.addCacheFile(new URI(args[2]), conf);
		job2.waitForCompletion(true);

		Configuration cf = new Configuration();
		HashMap<String, Integer> phaseTwoMap = new HashMap<String, Integer>();
		int ID = 0;
		HashMap<Integer, _Cluster> phaseTwolistOfClusters = new HashMap<Integer, _Cluster>();

		FileSystem fs = FileSystem.get(cf);
		FSDataInputStream in = fs.open(new Path(FileOutputFormat.getOutputPath(
				job2).toString()
				+ "/part-r-00000"));
		String strLine;
		while ((strLine = in.readLine()) != null) {

			_Association key = new _Association(strLine);

			_DataPoint P = key.getP();
			if (!phaseTwoMap.containsKey(P.toStringTwo()))
				phaseTwoMap.put(P.toStringTwo(), ID++);
			int PID = phaseTwoMap.get(P.toStringTwo());

			_DataPoint Q = key.getQ();
			if (!phaseTwoMap.containsKey(Q.toStringTwo()))
				phaseTwoMap.put(Q.toStringTwo(), ID++);
			int QID = phaseTwoMap.get(Q.toStringTwo());

			double distance = key.getDistance();
			_Cluster c1 = null, c2 = null;

			if (!phaseTwolistOfClusters.containsKey(PID)) {
				phaseTwolistOfClusters.put(PID, new _Cluster(P, PID));
				c1 = phaseTwolistOfClusters.get(PID);
			} else 
				c1 = phaseTwolistOfClusters.get(PID);

			if (!phaseTwolistOfClusters.containsKey(QID)) {
				phaseTwolistOfClusters.put(QID, new _Cluster(Q, QID));
				c2 = phaseTwolistOfClusters.get(QID);
			} else 
				c2 = phaseTwolistOfClusters.get(QID);

			double max = Math.max(c1.getAverageDistance(),
					c2.getAverageDistance());

			double minByK = Global.K
					* Math.min(c1.getAverageDistance(), c2.getAverageDistance());

			double newAvg;
			if ((distance < minByK) && (max < minByK)) {
				if (c1.getID() != c2.getID()) {

					newAvg = distance
							+ (c1.getAverageDistance() * c1.getSize())
							+ (c2.getAverageDistance() * c2.getSize());
					if (c1.getID() > c2.getID()) {
						HashSet<String> c1Associations = c1
								.getAssociationListSet();
						HashSet<String> c2Associations = c2
								.getAssociationListSet();
						for (String s : c2Associations) {
							if (!c1Associations.contains(s)) {
								_Association a = new _Association(s, 0);
								c1.insertAssociation(a);
							}
						}
						if (!c1Associations.contains(key)) {
							c1.insertAssociation(key);
						}
						c1.setAverageDistance(newAvg / c1.getSize());
						HashSet<String> c1DataPoints = c1.getDataPoints();
						HashSet<String> c2DataPoints = c2.getDataPoints();
						for (String i : c2DataPoints) {
							if (!c1DataPoints.contains(i))
								c1DataPoints.add(i);
							phaseTwoMap.put(i, c1.getID());
						}
						phaseTwolistOfClusters.put(c2.getID(), c1);

					} else {
						HashSet<String> c1Associations = c1
								.getAssociationListSet();
						HashSet<String> c2Associations = c2
								.getAssociationListSet();
						for (String s : c1Associations) {
							if (!c2Associations.contains(s)) {
								_Association a = new _Association(s, 0);
								c2.insertAssociation(a);
							}
						}
						if (!c2Associations.contains(key.toString())) {
							c2.insertAssociation(key);
						}
						c2.setAverageDistance(newAvg / c2.getSize());
						HashSet<String> c1DataPointsIDs = c1.getDataPoints();
						HashSet<String> c2DataPointsIDs = c2.getDataPoints();
						for (String i : c1DataPointsIDs) {
							if (!c2DataPointsIDs.contains(i))
								c2DataPointsIDs.add(i);
							phaseTwoMap.put(i, c2.getID());
						}
						phaseTwolistOfClusters.put(c1.getID(), c2);
					}
				} else {

					if (!c1.getAssociationListSet().contains(key)) {
						newAvg = distance
								+ (c1.getAverageDistance() * c1.getSize());
						c1.insertAssociation(key);
						c1.setAverageDistance(newAvg / c1.getSize());
					}

				}
			}

		}
		in.close();

		HashSet<Integer> differentClusterID = new HashSet<Integer>();
		Iterator it = phaseTwoMap.entrySet().iterator();
		ArrayList<_Cluster> differentClusters = new ArrayList<_Cluster>();
		while (it.hasNext()) {
			Map.Entry mapEntry = (Map.Entry) it.next();
			if (!differentClusterID.contains((Integer) mapEntry.getValue())) {
				differentClusterID.add((Integer) mapEntry.getValue());
				_Cluster c = phaseTwolistOfClusters.get((Integer) mapEntry
						.getValue());
				differentClusters.add(c);

			}
		}

		Collections.sort(differentClusters, new Comparator<_Cluster>() {

			@Override
			public int compare(_Cluster o1, _Cluster o2) {
				return (((o1.getSize() - o2.getSize())) * -1);

			}

		});
		phaseTwolistOfClusters = null;
		phaseTwoMap = null;
		int counter = 0;

		HashMap<String, Integer> finalMap = new HashMap<String, Integer>();
		HashMap<Integer, _Cluster> finalClusterList = new HashMap<Integer, _Cluster>();

		int finalID = 0;
		for (_Cluster current : differentClusters) {
			double harmonicScore = current.getHarmonicAverage();
			ArrayList<_Association> currentClusterAssociations = current
					.getAssociationList();
			Collections.sort(currentClusterAssociations,
					new Comparator<_Association>() {

						@Override
						public int compare(_Association o1, _Association o2) {
							if (o1.getDistance() > o2.getDistance())
								return 1;
							else if (o1.getDistance() < o2.getDistance())
								return -1;
							else
								return 0;
						}
					});
			for (_Association currentAssociation : currentClusterAssociations) {
				_DataPoint P = currentAssociation.getP();
				_DataPoint Q = currentAssociation.getQ();
				double distance = currentAssociation.getDistance();

				if (!finalMap.containsKey(P.toStringTwo()))
					finalMap.put(P.toStringTwo(), finalID++);
				int PID = finalMap.get(P.toStringTwo());

				if (!finalMap.containsKey(Q.toStringTwo()))
					finalMap.put(Q.toStringTwo(), finalID++);
				int QID = finalMap.get(Q.toStringTwo());

				_Cluster c1 = null, c2 = null;

				if (!finalClusterList.containsKey(PID)) {
					finalClusterList.put(PID, new _Cluster(P, PID));
					c1 = finalClusterList.get(PID);
				} else 
					c1 = finalClusterList.get(PID);
				
				if (!finalClusterList.containsKey(QID)) {
					finalClusterList.put(QID, new _Cluster(Q, QID));
					c2 = finalClusterList.get(QID);
				} else
					c2 = finalClusterList.get(QID);

				if (distance < Global.K * harmonicScore) {
					counter++;
					if (c1.getID() != c2.getID()) {
						if (c1.getID() > c2.getID()) {
							HashSet<String> c2DataPointsID = c2.getDataPoints();
							for (String data : c2DataPointsID) {
								if (!c1.getDataPoints().contains(data))
									c1.getDataPoints().add(data);
								finalMap.put(data, c1.getID());
							}
						} else {
							HashSet<String> c1DataPointsID = c1.getDataPoints();
							for (String data : c1DataPointsID) {
								if (!c2.getDataPoints().contains(data))
									c2.getDataPoints().add(data);
								finalMap.put(data, c2.getID());
							}
						}
					}
				}
				// add to list here
			}

		}
		//FileWriter fw = new FileW
		HashSet<Integer> finalClusterID = new HashSet<Integer>();
		ArrayList<_Cluster> list = new ArrayList<_Cluster>();
		Iterator it2 = finalMap.entrySet().iterator();
		while (it2.hasNext()) {
			Map.Entry mapEntry = (Map.Entry) it2.next();
			if (!finalClusterID.contains((Integer) mapEntry.getValue())) {
				finalClusterID.add((Integer) mapEntry.getValue());
				list.add(finalClusterList.get((Integer) mapEntry.getValue()));

			}
		}
		
/*
	*/


	}
	
}
