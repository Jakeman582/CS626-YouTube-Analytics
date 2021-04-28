package Utils;

import java.util.ArrayList;

/**
 * 
 * @author cloudera
 *
 */
public class Point {
	
	private static final int LABEL_INDEX = 0;
	private static final int COORDINATES_INDEX = 1;
	
	public static final String LABEL_DELIMITER = "\t";
	public static final String COORDINATE_DELIMITER = " ";
	
	public static String getLabelString(String line) {
		return line.split(Point.LABEL_DELIMITER)[Point.LABEL_INDEX];
	}
	
	public static String getCoordinateString(String line) {
		return line.split(Point.LABEL_DELIMITER)[Point.COORDINATES_INDEX];
	}
	
	public static ArrayList<Double> getCoordinateListFromCoordinateString(String line) {
		String[] temp = line.split(Point.COORDINATE_DELIMITER);
		ArrayList<Double> list = new ArrayList<Double>();
		for(String i : temp) {
			list.add(Double.parseDouble(i));
		}
		return list;
	}
	
	public static ArrayList<Double> getCoordinateListFromPointString(String line) {
		String[] coordinates = Point.getCoordinateString(line).split(Point.COORDINATE_DELIMITER);
		ArrayList<Double> list = new ArrayList<Double>();
		for(String i : coordinates) {
			list.add(Double.parseDouble(i));
		}
		return list;
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
