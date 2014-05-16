import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class _Cluster {
	private int ID;
	private HashSet<String> dataPoints;
	private ArrayList<_Association> associationList;
	private HashSet<String> associationListSet;
	private double avgDistance;
	private int size;

	public _Cluster(_DataPoint p,int ID) {
		this.ID = ID;
		associationList = new ArrayList<_Association>();
		dataPoints = new HashSet<String>();
		associationListSet = new HashSet<String>();
		dataPoints.add(p.toStringTwo());
		avgDistance = p.getTotalDistance() / p.getNumOfNeighbors();
	}
	public double getHarmonicAverage()
	{
		double sum = 0;
		for(String association : associationListSet)
		{
			_Association temp = new _Association(association, 0);
			sum+=(1/temp.getDistance());
		}
		
		return (size/sum);
	}

	public HashSet<String> getDataPoints() {
		return dataPoints;
	}

	public ArrayList<_Association> getAssociationList() {
		return associationList;
	}

	public HashSet<String> getAssociationListSet() {
		return associationListSet;
	}

	public double getAverageDistance() {
		return avgDistance;
	}

	public void setAverageDistance(double avgDistance) {
		this.avgDistance = avgDistance;
	}

	public int getID() {
		return ID;
	}

	public void insertAssociation(_Association a) {
		associationList.add(a);
		associationListSet.add(a.toString());
		size++;
	}

	public int getSize() {
		return size;
	}

	public String toString() {
		return "" + ID;
	}
}
