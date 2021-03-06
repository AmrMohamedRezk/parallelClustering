import java.awt.Color;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.xy.DefaultXYDataset;

/**
 * An SWT demo.
 */
public class SWTBarChartDemo {

	/**
	 * Creates a sample chart.
	 * 
	 * @param dataset
	 *            the dataset.
	 * 
	 * @return The chart.
	 */
	static JFreeChart createChart(ArrayList<_Cluster> clusters,
			HashMap<Integer, String> map, int size) {
		int clusterCount = 0;
		DefaultXYDataset dataset = new DefaultXYDataset();

		if (clusters != null) {
			for (_Cluster cluster : clusters) {
				HashSet<String> patterns = cluster.getDataPoints();
				// if (patterns.size() >= 0.01 * size) {
				double[][] value = new double[2][patterns.size()];
				int j=0;
				for (String s:patterns) {
					_DataPoint temp = new _DataPoint(s, 0);
						value[0][j] = temp.getX();
						value[1][j++] = temp.getY();
					
				}

				dataset.addSeries(clusterCount++, value);
				// }
			}
		}
		JFreeChart chart = ChartFactory.createScatterPlot("SWTBarChartDemo1", // chart
																				// title
				"Category", // domain axis label
				"Value", // range axis label
				dataset, // data
				PlotOrientation.VERTICAL, // orientation
				true, // include legend
				true, // tooltips?
				false // URLs?
				);

		// NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
		chart.clearSubtitles();
		// get a reference to the plot for further customisation...
		XYPlot plot = (XYPlot) chart.getPlot();

		// set the range axis to display integers only...
		NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setRange(0, 450);
		NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
		domainAxis.setRange(0, 800);
		Color brown = new Color(156, 93, 82);
		Color maroon = new Color(139, 28, 98);
		XYItemRenderer renderer = plot.getRenderer();
		Color[] colors = { Color.red, Color.blue, Color.BLACK, Color.green,
				maroon, brown, Color.yellow, Color.MAGENTA, Color.white,
				Color.pink, Color.cyan };
		int i = 0;
		if (clusters != null) {
			for (_Cluster c : clusters) {
				renderer.setSeriesPaint(i, colors[i % colors.length]);
				i++;
			}
		}
		return chart;

	}

	/**
	 * Starting point for the demonstration application.
	 * 
	 * @param args
	 *            ignored.
	 */
	public static void main(String[] args) {

	}

}
