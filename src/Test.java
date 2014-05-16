import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.jfree.chart.JFreeChart;
import org.jfree.experimental.chart.swt.ChartComposite;

public class Test {
	public static HashMap<Integer, String> loadDataFromFile(String fileName)
			throws IOException {
		HashMap<Integer, String> patternMap = new HashMap<Integer, String>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line = null;
		int i = 1;
		while ((line = reader.readLine()) != null) {
			patternMap.put(i++, line);
		}
		reader.close();
		return patternMap;
	}

	public static void main(String[] args) {
		 HashMap<String, Integer> phaseTwoMap= new HashMap<String, Integer>();
		 int ID=0;
		 HashMap<Integer, _Cluster> phaseTwolistOfClusters=new HashMap<Integer, _Cluster>();
		
		FileInputStream fs;
		try {
			fs = new FileInputStream("file.txt");
			DataInputStream in = new DataInputStream(fs);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
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
				} else {
					c1 = phaseTwolistOfClusters.get(PID);
					// while (c1.getID() !=
					// listOfClusters.get(c1.getID()).getID())
					// c1 = listOfClusters.get(c1.getID());
				}

				if (!phaseTwolistOfClusters.containsKey(QID)) {
					phaseTwolistOfClusters.put(QID, new _Cluster(Q, QID));
					c2 = phaseTwolistOfClusters.get(QID);
				} else {
					c2 = phaseTwolistOfClusters.get(QID);
					// while (c2.getID() !=
					// listOfClusters.get(c2.getID()).getID())
					// c2 = listOfClusters.get(c2.getID());
				}

				double max = Math.max(c1.getAverageDistance(),
						c2.getAverageDistance());

				double minByK = Global.K
						* Math.min(c1.getAverageDistance(),
								c2.getAverageDistance());

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
							HashSet<String> c1DataPointsIDs = c1
									.getDataPoints();
							HashSet<String> c2DataPointsIDs = c2
									.getDataPoints();
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
					// TODO Auto-generated method stub

				}

			});
			phaseTwolistOfClusters=null;
			phaseTwoMap= null;
			int counter =0;
			
			HashMap<String, Integer> finalMap = new HashMap<String, Integer>();
			HashMap<Integer, _Cluster> finalClusterList = new HashMap<Integer, _Cluster>();
			
			int finalID = 0;
			for(_Cluster current : differentClusters)
			{
				double harmonicScore = current.getHarmonicAverage();
				ArrayList<_Association> currentClusterAssociations = current.getAssociationList();
				Collections.sort(currentClusterAssociations,new Comparator<_Association>() {

					@Override
					public int compare(_Association o1, _Association o2) {
									if(o1.getDistance()>o2.getDistance() )
										return 1;
									else if (o1.getDistance()<o2.getDistance())
										return -1;
									else return 0;
					}
				});
				for (_Association currentAssociation:currentClusterAssociations)
				{
					_DataPoint P = currentAssociation.getP();
					_DataPoint Q = currentAssociation.getQ();
					double distance = currentAssociation.getDistance();
					
					if(!finalMap.containsKey(P.toStringTwo()))
						finalMap.put(P.toStringTwo(), finalID++);
					int PID = finalMap.get(P.toStringTwo());
					
					if(!finalMap.containsKey(Q.toStringTwo()))
						finalMap.put(Q.toStringTwo(), finalID++);
					int QID = finalMap.get(Q.toStringTwo());

					_Cluster c1 = null, c2 = null;

					if (!finalClusterList.containsKey(PID)) {
						finalClusterList.put(PID, new _Cluster(P, PID));
						c1 = finalClusterList.get(PID);
					} else {
						c1 = finalClusterList.get(PID);
						// while (c1.getID() !=
						// listOfClusters.get(c1.getID()).getID())
						// c1 = listOfClusters.get(c1.getID());
					}

					if (!finalClusterList.containsKey(QID)) {
						finalClusterList.put(QID, new _Cluster(Q, QID));
						c2 = finalClusterList.get(QID);
					} else {
						c2 = finalClusterList.get(QID);
						// while (c2.getID() !=
						// listOfClusters.get(c2.getID()).getID())
						// c2 = listOfClusters.get(c2.getID());
					}
	
					if(distance<Global.K*harmonicScore)
					{
						counter++;
						if(c1.getID()!=c2.getID())
						{
							if(c1.getID() > c2.getID())
							{
								HashSet<String> c2DataPointsID = c2.getDataPoints();
								for(String data:c2DataPointsID )
								{
									if(!c1.getDataPoints().contains(data))
										c1.getDataPoints().add(data);
									finalMap.put(data, c1.getID());
								}
							}else{
								HashSet<String> c1DataPointsID = c1.getDataPoints();
								for(String data:c1DataPointsID )
								{
									if(!c2.getDataPoints().contains(data))
										c2.getDataPoints().add(data);
									finalMap.put(data, c2.getID());
								}
							}
						}
					}
					//add to list here
				}
				
				
			}
			System.out.println("COinter is " +counter);
		
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
			HashMap<Integer, String> patternsMap = 
					loadDataFromFile("t4.8k.dat");
			//phase3.outlierHandling(phase3.clusters, patternNo, points);

			Display display = new Display();
			JFreeChart chart1 = SWTBarChartDemo.createChart(
					list, patternsMap, 8000);
			Shell shell1 = new Shell(display);
			shell1.setSize(1000, 800);
			shell1.setLayout(new FillLayout());
			shell1.setText("Draw Mitosis Clusters");
			ChartComposite frame1 = new ChartComposite(shell1, SWT.NONE,
					chart1, true);

			frame1.pack();
			shell1.open();
			while (!shell1.isDisposed()) {
				if (!display.readAndDispatch())
					display.sleep();
			}

			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * 
		 * 
		 * }
		 */
	}
}
