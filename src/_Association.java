import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class _Association implements Writable,WritableComparable<_Association>{
	private _DataPoint p;
	private _DataPoint q;
	private double distance;
	
	public _Association()
	{
		
	}
	
	public _Association(_DataPoint p, _DataPoint q, double distance) {
		this.p = p;
		this.q = q;
		this.distance = distance;
	}
	
	public _Association(String s)
	{
		StringTokenizer st = new StringTokenizer(s,"$");
		this.p= new _DataPoint(st.nextToken(),2);
		this.q= new _DataPoint(st.nextToken(),2);
		String s2=st.nextToken();
		this.distance = Double.parseDouble(s2.substring(0,s2.indexOf('\t')));
		

	}
	public _Association(String s,int i)
	{
		StringTokenizer st = new StringTokenizer(s,"$");
		this.p= new _DataPoint(st.nextToken(),2);
		this.q= new _DataPoint(st.nextToken(),2);
		this.distance = Double.parseDouble(st.nextToken());
		

	}

	@Override
	public int compareTo(_Association o) {
		if(this.equals(o))
			return 0;
		else 
		{
			double temp = this.distance-o.distance;
			return temp>0 ? 1:-1;
		}
		
	}

	public boolean equals(Object o) {
		_Association other = (_Association) o;
		return (this.p.equals(other.p) && this.q.equals(other.q))
				|| (this.p.equals(other.q) && this.q.equals(other.p));
	}

	public _DataPoint getP() {
		return p;
	}

	public void setP(_DataPoint p) {
		this.p = p;
	}

	public _DataPoint getQ() {
		return q;
	}

	public void setQ(_DataPoint q) {
		this.q = q;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	public String toString()
	{
		return p.toStringTwo()+"$"+q.toStringTwo()+"$"+distance;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String input = in.readLine();
		StringTokenizer st = new StringTokenizer(input,"$");
		this.p= new _DataPoint(st.nextToken(),2);
		this.q= new _DataPoint(st.nextToken(),2);
		this.distance = Double.parseDouble(st.nextToken());
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBytes(this.toString());
		
	}

	
}
