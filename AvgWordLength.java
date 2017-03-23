package stubs;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class AvgWordLength extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new AvgWordLength(), args);
	System.exit(res);
  }

@Override
public int run(String[] args) throws Exception {
	 	Configuration conf = this.getConf();
	 	/*
	 	 * 
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.client.model.geojson.Geometry;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.geojson.*;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBAddress;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
public class hi {
	public static void main(String[] args) throws FileNotFoundException{
		
		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("flightdb");
		MongoCollection<Document> collection = database.getCollection("test");
		List<Document> documents = new ArrayList<Document>();		
			Scanner scanner = new Scanner(new File("/home/training/Downloads/airports.dat"));
			scanner.useDelimiter("\n");
			while (scanner.hasNext()) {
			    String line = scanner.next();
			 
			    String[] p = line.split(",");
			    if (p.length == 14)
			    addAirport(documents, p[1],p[4],Double.parseDouble(p[6]),Double.parseDouble(p[7]));
		    }
	  
		collection.insertMany(documents);
		collection.createIndex(Indexes.geo2dsphere("location"));
		mongoClient.close();
	}
	private static void addAirport(List<Document> l, String name, String code,Double lat, Double lon){
		if (code.equals("\\N")) return;
		name = name.substring(1, name.length()-1);
		code = code.substring(1, code.length()-1);
		Document doc = new Document("name", name)
        .append("IATA_Code", code)
        .append("location", new Point(new Position(lon,lat)));
		l.add(doc);	
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


	private static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}


	private static double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	} 

	
}
	 	 * 
	 	 * */
	 	Job job = new Job(conf, "My Job");
	 	
	 	

	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running 
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(AvgWordLength.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */


	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("temp"));
	    job.setMapperClass(LetterMapper.class);
	    job.setReducerClass(AverageReducer.class); 
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    boolean success = job.waitForCompletion(true);
	    
	    //job2.set Mapper to same (LetterMapper)
	    //
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    Job job2 = new Job(conf, "temp");
	    job2.setJarByClass(AvgWordLength.class);
	    FileInputFormat.setInputPaths(job2, new Path("temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	    job2.setMapperClass(LetterMapper.class);
	    job2.setReducerClass(AverageReducer2.class); 
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);	    
	      return job2.waitForCompletion(true) ? 0 : 1;
	    }
}

