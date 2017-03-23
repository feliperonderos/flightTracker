package stubs;
import java.io.IOException;
import com.mongodb.MongoClient;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;

import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;

public class AverageReducer extends Reducer<Text, Text, Text, Text> {
	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection;
	final Logger l = Logger.getLogger (AverageReducer.class.getName());
	public void setup(Context context) {
		l.setLevel(Level.WARN); 
		mongoClient = new MongoClient();
		database = mongoClient.getDatabase("flightdb");
		collection = database.getCollection("test");
	}

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

		
	  
	  TreeSet<String[]> set = new TreeSet<String[]>(new Comparator<String[]>(){
		@Override
		public int compare(String[] o1, String[] o2) {
			return (new Integer(o1[3]) - new Integer(o2[3]));
		}
	  });
	  String[] arr = new String[7];
	  for (Text val: values){
		  String str = val.toString();
		  arr = str.split(" ");
		  if (arr[3].matches("\\d+")){
			  set.add(arr);  
		  }
	  }
	  
	  
	  boolean trim = false;
	  boolean onGround = false;
	  String[] takeoff = null;
	  String[] last = null;
	  for (String[] s : set) {
		  if (last == null) last = s;
		  
		  if (!trim){
			  if (s[0].equals("True")){
				  trim = true;
				  onGround = true;
			  }
			  
			  if (
					  ((Long.parseLong(s[3]) - Long.parseLong(last[3])) > 3600)&&
					  (distance(Double.parseDouble(s[1]),
							Double.parseDouble(s[2]),
							Double.parseDouble(last[1]),
							Double.parseDouble(last[2]),
							"K")<20)&&
					 (!s[4].equals("None"))&&
					 (!last[4].equals("None"))&&
				     (Double.parseDouble(s[4]) < 3300)&&
				     (Double.parseDouble(last[4]) < 3300)){
				  takeoff = s;
				  trim = true;
				  onGround = false;
				  
			  }
			  
		  }
		  else if (onGround){
			  if (s[0].equals("False")){
				  takeoff = s; 
				  onGround = false;
			  }
		  }
		  else{
			  if(s[0].equals("True")){
				  addFlight(collection,context,takeoff,s);
				  onGround = true;
				  
			  }
			  if (
					  ((Long.parseLong(s[3]) - Long.parseLong(last[3])) > 3600)&&
					  (distance(Double.parseDouble(s[1]),
							Double.parseDouble(s[2]),
							Double.parseDouble(last[1]),
							Double.parseDouble(last[2]),
							"K")<20)&&
							(!s[4].equals("None"))&&
							 (!last[4].equals("None"))&&
				     (Double.parseDouble(s[4]) < 3300)&&
				     (Double.parseDouble(last[4]) < 3300)){
				  addFlight(collection,context,takeoff,last);
				  takeoff = s;
			  }
			  
			  
		  }
				 
		  
		  last = s;
		  
		  
	  }
		  
		  
		  
		  
		  
		  
		  
		  
		 /* 
		  if difference between this and last > 45min and location within 20 km
		  
		  and
		  
		  below 10000feet then 
		  if takeoff not null add last as landing and add flight
		  if takeoff is null then trim == true
		  
		  add this as takeoff
		  
		  
		  
		  
		  
		  
		  
		  if (!trim){
			  if (s[0].equals("True")){
				  trim = true;
				  onGround = true;
			  }
		  }
		  else if (onGround){
			  if (s[0].equals("False")){
				  takeoff = s; 
				  onGround = false;
			  }
		  }
		  else{
			  if(s[0].equals("True")){
				  addFlight(collection,context,takeoff,s);
				  onGround = true;
				  
			  }
		  }
		  	  
	  }
	  
	  
	  
	  
	  /*boolean trim = false;
	  boolean onGround = false;
	  String[] takeoff = null;
	  for (String[] s : set) {
		  if (!trim){
			  if (s[0].equals("True")){
				  trim = true;
				  onGround = true;
			  }
		  }
		  else if (onGround){
			  if (s[0].equals("False")){
				  takeoff = s; 
				  onGround = false;
			  }
		  }
		  else{
			  if(s[0].equals("True")){
				  addFlight(collection,context,takeoff,s);
				  onGround = true;
				  
			  }
		  }
		  	  
	  }*/
	 
  }
  public void cleanup(Context context) throws
  IOException, InterruptedException {
	  mongoClient.close();
	  
  }
  private static boolean checkDigitString(String s){
	  return (s!=null && s.matches("\\d+"));
  }
  private static boolean checkDigitString(String[] a){
	  for (String s: a){
	    if (!checkDigitString(s)) return false;
	    
	  }
	  return true;
  }
  private void addFlight(MongoCollection<Document> collection, Context context, String[] takeoff, String[] landing) throws IOException, InterruptedException {
	  //checkDistance, set airports, write airports, time in seconds
	  
		String takeoffCode, landingCode;
		Long time = Long.parseLong(landing[3]) - Long.parseLong(takeoff[3]);
		int distance = (int) distance(Double.parseDouble(landing[1]),
				Double.parseDouble(landing[2]),
				Double.parseDouble(takeoff[1]),
				Double.parseDouble(takeoff[2]),
				"K");
		//check if faster than 1100km/h or slower than 400km/h + 3hours
		//if ((distance/(time/3600.0) > 1100)||
		//    (distance/((time-10000)/3600.0)< 400))return;
		if (distance < 200) return;
		int speed = (int) (distance/(time/3600.0));
		if (speed > 1100) return;
		if ((distance < 2000)&&(speed < 330)) return;
		if ((distance < 4000)&&(speed < 500)) return;
		else if ((distance > 3999)&& (speed < 600))return;
		
		
	   
		
		
		Point refPoint = new Point(new Position(Double.parseDouble(takeoff[2]),
				Double.parseDouble(takeoff[1])));
		Document d = collection.find(Filters.near("location", refPoint, 10000.0, 0.0)).first();
		if (d == null) return; 
		takeoffCode = d.getString("IATA_Code");
		refPoint = new Point(new Position(Double.parseDouble(landing[2]),
				Double.parseDouble(landing[1])));
		d = collection.find(Filters.near("location", refPoint, 10000.0, 0.0)).first();
		if (d == null) return; 
		landingCode = d.getString("IATA_Code");
		if (takeoffCode.compareTo(landingCode) > 0)
			context.write(new Text(takeoffCode + "-" + landingCode), new Text(((Long)(time * -1)).toString()));
		else
			context.write(new Text(landingCode + "-" + takeoffCode), new Text((time).toString()));
			
	
  }
  private static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == "K") {
			dist = dist * 1.609344;
		} else if (unit == "N") {
			dist = dist * 0.8684;
		}

		return (dist);
	}

	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	/*::	This function converts decimal degrees to radians						 :*/
	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	private static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	/*::	This function converts radians to decimal degrees						 :*/
	/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
	private static double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	} 
 
}