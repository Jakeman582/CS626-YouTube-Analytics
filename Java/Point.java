package Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author cloudera
 *
 */
public class Point implements WritableComparable<Point> {
	
	private Text label;
	private ArrayPrimitiveWritable coordinates;
	
	private final int LABEL_INDEX = 0;
	private final int COORDINATES_INDEX = 1;
	
	public static final String LABEL_DELIMITER = "\t";
	public static final String COORDINATE_DELIMITER = " ";
	
	
	/* Constructors */
	public Point() {
		this.label = new Text();
		this.coordinates = new ArrayPrimitiveWritable(double.class);
	}
	
	public Point(Text newLabel, ArrayPrimitiveWritable newCoordinates) {
		this.label = newLabel;
		this.coordinates = newCoordinates;
	}
	
	public Point(String newLabel, double[] newCoordinates) {
		this.label = new Text(newLabel);
		this.coordinates = new ArrayPrimitiveWritable(newCoordinates);
	}
	
	public Point(String line) {
		this.parseLine(line);
	}

	/* I/O */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.label.readFields(in);
		this.coordinates.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.label.write(out);
		this.coordinates.write(out);
	}
	
	/* Comparison */
	@Override
	public boolean equals(Object other) {
		if(other instanceof Point) {
			Point otherPoint = (Point) other;
			return this.label.equals(otherPoint.label) && this.coordinates.equals(otherPoint.coordinates);
		}
		return false;
	}
	
	@Override
	public int compareTo(Point otherPoint) {
		return this.label.compareTo(otherPoint.label);
	}
	
	/* Hash code */
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
	
	/* String */
	public String getCoordinateString() {
		StringBuilder coordinateString = new StringBuilder();
		double[] tempArray = (double[]) this.coordinates.get();
		for(int index = 0; index < tempArray.length; index++) {
			coordinateString.append(tempArray[index]);
			if(index < tempArray.length - 1) {
				coordinateString.append(Point.COORDINATE_DELIMITER);
			}
		}
		return coordinateString.toString();
	}
	
	@Override
	public String toString() {
		return this.label.toString() + Point.LABEL_DELIMITER + this.getCoordinateString();
	}
	
	/* Getters */
	public Text getLabel() { return this.label; }
	public double[] getCoordinates() {return (double[]) this.coordinates.get(); }
	public ArrayList<Double> getCoordinatesArrayList() {
		ArrayList<Double> coordinatesList = new ArrayList<Double>();
		for(double i : this.getCoordinates()) {
			coordinatesList.add(i);
		}
		return coordinatesList;
	}
	
	/* Setters */
	public void setLabel(String newLabel) { this.label = new Text(newLabel); }
	public void setCoordinates(double[] newCoordinates) { this.coordinates = new ArrayPrimitiveWritable(newCoordinates); }

	/* Parsing */
	public void parseLine(String line) {
		
		// Get each part of the point
		String[] parts = line.split(Point.LABEL_DELIMITER);
		this.setLabel(parts[LABEL_INDEX]);
		this.setCoordinates(Point.parseCoordinateString(parts[COORDINATES_INDEX]));
		
	}
	
	public static double[] parseCoordinateString(String lineCoordinates) {
		String[] temp = lineCoordinates.split(Point.COORDINATE_DELIMITER);
		double[] newCoordinates = new double[temp.length];
		for(int index = 0; index < temp.length; index++) {
			newCoordinates[index] = Double.parseDouble(temp[index]);
		}
		return newCoordinates;
	}
	
	public static String stringifyCoordinateList(ArrayList<Double> coordinateList) {
		StringBuilder coordinateString = new StringBuilder();
		for(int index = 0; index < coordinateList.size(); index++) {
			coordinateString.append(coordinateList.get(index));
			if(index < coordinateList.size() - 1) {
				coordinateString.append(Point.COORDINATE_DELIMITER);
			}
		}
		return coordinateString.toString();
	}
	
}
