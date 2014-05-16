import java.util.StringTokenizer;
import java.util.StringTokenizer;

public class _DataPoint {

	/*
	 * For now this class will be a 2d point this is to be changed to the vector
	 * but the interfaces will remain the same
	 */

	private double x;
	private double y;
	private double minDistance;
	private double totalDistance;
	private int numOfNearestNeighbors;

	public _DataPoint(String point, int indicator) {
		StringTokenizer st = new StringTokenizer(point);
		if (indicator == 0) {
			x = Double.parseDouble(st.nextToken());
			y = Double.parseDouble(st.nextToken());
			minDistance = Double.MAX_VALUE;

		} else if (indicator == 1) {
			x = Double.parseDouble(st.nextToken());
			y = Double.parseDouble(st.nextToken());
			minDistance = Double.MAX_VALUE;
			setMinDistance(Double.parseDouble(st.nextToken()));

		} else if (indicator == 2) {
			x = Double.parseDouble(st.nextToken());
			y = Double.parseDouble(st.nextToken());
			minDistance = Double.MAX_VALUE;
			setMinDistance(Double.parseDouble(st.nextToken()));
			totalDistance = Double.parseDouble(st.nextToken());
			numOfNearestNeighbors = Integer.parseInt(st.nextToken());
		}

	}

	public _DataPoint(double x, double y) {
		this.x = x;
		this.y = y;
		minDistance = Double.MAX_VALUE;
	}

	public String toStringZero()
	{
		return x+" "+y;
	}
	public String toStringOne()
	{
		return x+" "+y+" "+minDistance;
	}

	public String toStringTwo()
	{
		return x+" "+y+" "+minDistance+" "+totalDistance+" "+numOfNearestNeighbors;
	}


	public boolean equals(Object other) {
		_DataPoint oth = (_DataPoint) other;
		return this.x == oth.x && this.y == oth.y;
	}

	public double calculateDistance(_DataPoint other) {
		if (this.equals(other)) {
			return Double.MAX_VALUE;
		}
		double deltaX = Math.abs(this.x - other.x);
		double deltaY = Math.abs(this.y - other.y);
		return Math.sqrt(Math.pow(deltaX, 2) + Math.pow(deltaY, 2));
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getMinDistance() {
		return minDistance;
	}

	public void setMinDistance(double minDistance) {
		if (minDistance < this.minDistance)
			this.minDistance = minDistance;
	}
	public double getTotalDistance()
	{
		return totalDistance;
	}
	public void addDistance(double distance)
	{
		totalDistance+=distance;
	}
	public int getNumOfNeighbors()
	{
		return numOfNearestNeighbors;
	}
	public void incrementNeighbors()
	{
		numOfNearestNeighbors++;
	}

	
}
