package stubs;
import static com.mongodb.client.model.Filters.eq;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
//sort -r -n -k 2,2 _user_training_output6_part-r-00000 > sorted_output

public class AverageReducer2 extends Reducer<Text, Text, Text, Text> {
	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection;
	final Logger l = Logger.getLogger (AverageReducer2.class.getName());
	public void setup(Context context) {
		l.setLevel(Level.WARN); 
		mongoClient = new MongoClient();
		database = mongoClient.getDatabase("flightdb");
		collection = database.getCollection("test");
	}

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  String keyS = key.toString();
	  int curr, inF, outF;
	  int minSample = 3;
	  long[] out = {0,0};
	  long[] in = {0,0};
	  for (Text val: values){
		  try{
			  curr = Integer.parseInt(val.toString());
			  if (curr > 0){
				  out[0] = out[0] + curr;
				  out[1] = out[1] + 1;			  
			  }
			  else {
				  in[0] = in[0] - curr;
				  in[1] = in[1] + 1;			 
			  }
		  }
		  catch (Exception e){
		  }
	  }
	  if (in[1]< minSample || out[1] < minSample) return;
	  inF =  (int) (in[0]/in[1]);
	  outF =  (int) (out[0]/out[1]);
	  Integer F = inF - outF;
	  if (F < 0){
		  F *= -1;
		  keyS = keyS.subSequence(4,7 ).toString() + '-' + keyS.subSequence(0, 3).toString();
	  }
	  Document airport1 = collection.find(eq("IATA_Code",keyS.subSequence(0, 3).toString())).first();
	  Document airport2 = collection.find(eq("IATA_Code",keyS.subSequence(4, 7).toString())).first();
	  ArrayList<Double> loc1 =  (ArrayList<Double>)(((Document)(airport1.get("location")) ).get("coordinates"));
	  ArrayList<Double> loc2 =  (ArrayList<Double>)(((Document)(airport2.get("location")) ).get("coordinates"));
	  Double dist = distance(loc1.get(1),loc1.get(0),loc2.get(1),loc2.get(0), "K");
	  Double bearing = bearing(loc1.get(1),loc1.get(0),loc2.get(1),loc2.get(0));
	  Double eastWest = Math.sin(bearing) * dist;
	  
	  context.write(new Text(keyS), new Text(F.toString() +" "+dist.toString() + " "+ eastWest.toString()));
  }
  
  public void cleanup(Context context) throws
  IOException, InterruptedException {
	  mongoClient.close();
	  
  }
  protected static double bearing(double lat1, double lon1, double lat2, double lon2){
	  double longitude1 = lon1;
	  double longitude2 = lon2;
	  double latitude1 = Math.toRadians(lat1);
	  double latitude2 = Math.toRadians(lat2);
	  double longDiff= Math.toRadians(longitude2-longitude1);
	  double y= Math.sin(longDiff)*Math.cos(latitude2);
	  double x=Math.cos(latitude1)*Math.sin(latitude2)-Math.sin(latitude1)*Math.cos(latitude2)*Math.cos(longDiff);

	  return (Math.toDegrees(Math.atan2(y, x))+360)%360;
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