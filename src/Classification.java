/**
 * Created by Michael on 11/2/15.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.File;
import java.util.*;


public final class Classification {

    public static int numClust = 0;

    public static JavaRDD<Cluster> centroid(JavaRDD<String> lines, final int maxClusters) {
//        SparkConf sparkConfa = new SparkConf().setAppName("CentroidCluster").setMaster("local[1]");
//        JavaSparkContext ctxa = new JavaSparkContext(sparkConfa);
//        JavaRDD<String> lines = ctxa.textFile(fileName, 1);
        JavaRDD<Cluster> clusters = lines.flatMap(new FlatMapFunction<String, Cluster>() {
            @Override
            public Iterable<Cluster> call(String s) {
                Cluster[] centroids = new Cluster[maxClusters];
                Cluster[] centroids_ref = new Cluster[maxClusters];
                int i, k, index = 0;
                Review rv;
                String reviews; //= new String(""); //Redundant declaration
                String SingleRv; //= new String(""); //Redundant declaration
                //File modelFile;
                Scanner opnScanner;
                for (i = 0; i < maxClusters; i++) {
                    centroids[i] = new Cluster();
                    centroids_ref[i] = new Cluster();
                }
                //modelFile = new File(strModelFile);
                opnScanner = new Scanner(s);
                while (opnScanner.hasNext()) {
                    k = opnScanner.nextInt();
                    centroids_ref[k].similarity = opnScanner.nextFloat();
                    centroids_ref[k].movie_id = opnScanner.nextLong();
                    centroids_ref[k].total = opnScanner.nextShort();
                    reviews = opnScanner.next();
                    Scanner revScanner = new Scanner(reviews).useDelimiter(",");
                    while (revScanner.hasNext()) {
                        SingleRv = revScanner.next();
                        index = SingleRv.indexOf("_");
                        String reviewer = new String(SingleRv.substring(0, index));
                        String rating = new String(SingleRv.substring(index + 1));
                        rv = new Review();
                        rv.rater_id = Integer.parseInt(reviewer);
                        rv.rating = Integer.parseInt(rating);
                        centroids_ref[k].reviews.add(rv);
                    }
                }
                // implementing naive bubble sort as maxClusters is small
                // sorting is done to assign top most cluster ids in each iteration
                for (int pass = 1; pass < maxClusters; pass++) {
                    for (int u = 0; u < maxClusters - pass; u++) {
                        if (centroids_ref[u].movie_id < centroids_ref[u + 1].movie_id) {
                            Cluster temp = new Cluster(centroids_ref[u]);
                            centroids_ref[u] = centroids_ref[u + 1];
                            centroids_ref[u + 1] = temp;
                        }
                    }
                }
                for (int l = 0; l < maxClusters; l++) {
                    if (centroids_ref[l].movie_id != -1) {
                        centroids_ref[l].clusterID = l;
                        numClust++;
                    }
                }
                return Arrays.asList(centroids_ref);
            }
        });
        return clusters;
    }



    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Classification").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        java.sql.Timestamp startTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
        long startTime = System.nanoTime();
        JavaRDD<String> lines = ctx.textFile("initial_centroids", 1);
        JavaRDD<Cluster> centroids = centroid(lines, 16);
        long count = centroids.count();
        //System.out.println("Total number is " + count);
        //System.out.println("Numclust is " + numClust);
        assert(count == 16);//Total of 16 categories
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> list = centroids.flatMapToPair(new PairFlatMapFunction<Cluster, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Iterable<Tuple2<Integer, Tuple2<Integer, Integer>>> call(Cluster cluster) throws Exception {
                List<Tuple2<Integer, Tuple2<Integer, Integer>>> values = new ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>>();
                for (int i = 0; i < cluster.total; i++) {
                    Tuple2<Integer, Integer> subelem = new Tuple2<Integer, Integer>(cluster.reviews.get(i).rater_id, cluster.reviews.get(i).rating);
                    Tuple2<Integer, Tuple2<Integer, Integer>> elem = new Tuple2<Integer, Tuple2<Integer, Integer>>(cluster.clusterID, subelem);
                    values.add(elem);
                }
                return values;
            }
        });

        count = list.count();
        assert(count == 8737);//Total of 8737 reviews of all 16 centroid movies
        //System.out.println("Total number is " + count);

        lines = ctx.textFile("file1", 1);
        JavaPairRDD<Integer, Tuple2<Long, Integer>> movies = lines.flatMapToPair(new PairFlatMapFunction<String, Integer, Tuple2<Long, Integer>>() {
            @Override
            public Iterable<Tuple2<Integer, Tuple2<Long, Integer>>> call(String s) throws Exception {
                List<Tuple2<Integer, Tuple2<Long, Integer>>> movieList = new ArrayList<Tuple2<Integer, Tuple2<Long, Integer>>>();
                String movieIdStr = new String();
                String reviewStr = new String();
                String userIdStr = new String();
                String reviews = new String();
                String line = new String();
                String tok = new String("");
                long movieId;
                int review, userId, p,q,r,rater,rating,movieIndex;
                Cluster movie = new Cluster();
                movieIndex = s.indexOf(":");
                if (movieIndex > 0) {
                    movieIdStr = s.substring(0, movieIndex);
                    movieId = Long.parseLong(movieIdStr);
                    movie.movie_id = movieId;
                    reviews = s.substring(movieIndex + 1);
                    StringTokenizer token = new StringTokenizer(reviews, ",");

                    while (token.hasMoreTokens()) {
                        tok = token.nextToken();
                        int reviewIndex = tok.indexOf("_");
                        userIdStr = tok.substring(0, reviewIndex);
                        reviewStr = tok.substring(reviewIndex + 1);
                        userId = Integer.parseInt(userIdStr);
                        review = Integer.parseInt(reviewStr);

                        Tuple2<Long, Integer> subelem1 = new Tuple2<Long, Integer>(movieId, review);
                        Tuple2<Integer, Tuple2<Long, Integer>> elem1 = new Tuple2<Integer, Tuple2<Long, Integer>>(userId, subelem1);
                        movieList.add(elem1);
                    }
                }
                return movieList;
            }
        });

        /*
        JavaPairRDD<Tuple2<Long, Integer>, Integer> movies2 = lines.flatMapToPair(new PairFlatMapFunction<String, Tuple2<Long, Integer>, Integer>() {
            @Override
            public Iterable<Tuple2<Tuple2<Long, Integer>, Integer>> call(String s) throws Exception {
                List<Tuple2<Tuple2<Long, Integer>, Integer>> movieList2 = new ArrayList<Tuple2<Tuple2<Long, Integer>, Integer>>();
                String movieIdStr = new String();
                String reviewStr = new String();
                String userIdStr = new String();
                String reviews = new String();
                String line = new String();
                String tok = new String("");
                long movieId;
                int review, userId, p,q,r,rater,rating,movieIndex;
                Cluster movie = new Cluster();
                movieIndex = s.indexOf(":");
                if (movieIndex > 0) {
                    movieIdStr = s.substring(0, movieIndex);
                    movieId = Long.parseLong(movieIdStr);
                    movie.movie_id = movieId;
                    reviews = s.substring(movieIndex + 1);
                    StringTokenizer token = new StringTokenizer(reviews, ",");

                    while (token.hasMoreTokens()) {
                        tok = token.nextToken();
                        int reviewIndex = tok.indexOf("_");
                        userIdStr = tok.substring(0, reviewIndex);
                        reviewStr = tok.substring(reviewIndex + 1);
                        userId = Integer.parseInt(userIdStr);
                        review = Integer.parseInt(reviewStr);

                        Tuple2<Long, Integer> subelem2 = new Tuple2<Long, Integer>(movieId, userId);
                        Tuple2<Tuple2<Long, Integer>, Integer> elem2 = new Tuple2<Tuple2<Long, Integer>, Integer>(subelem2, review);
                        movieList2.add(elem2);
                    }
                }
                return movieList2;
            }
        });
        */
        count = movies.count();
        System.out.println("The total number of movies is " + count);

        /*
        //Long count2 = movies2.count();
        //System.out.println("movies2 is " + count2);
        //The exact number of pairs is currently unknown...TOO BIG TO COUNT MANUALLY.

        //Object jm = movies2.join(movies2);

        //JavaPairRDD<Tuple2<Long, Integer>,Tuple2<Integer, Integer> > joinedMovies = (JavaPairRDD<Tuple2<Long, Integer>,Tuple2<Integer, Integer> >) jm;
        //count = joinedMovies.count();
        //System.out.println("JoinedMovies is " + count);
        //JavaPairRDD<Tuple2<Long, Integer>,Tuple2<Integer, Integer> > distinctMovie = joinedMovies.distinct();
        //count = distinctMovie.count();
        //System.out.println("distinctMovie is " + count);
        */

        JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> listGroupedByClusterID = list.groupByKey();
        count = listGroupedByClusterID.count();
        System.out.println("listGroupedByRaterID is " + count);

        JavaPairRDD<Integer, Double> averaged = listGroupedByClusterID.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Double>() {
            @Override
            public Double call(Iterable<Tuple2<Integer, Integer>> tuple2s) throws Exception {
                Double total = 0.0;
                int num = 0;
                Iterator<Tuple2<Integer, Integer>> iter = tuple2s.iterator();
                while(iter.hasNext()) {
                    int current = iter.next()._2();
                    num++;
                    total += current;
                }
                Double average = total / num;
                return average;
            }
        });

        count = averaged.count();
        //System.out.println("averaged is " + count);
        assert(count == 16);

        long endTime = System.nanoTime();
        java.sql.Timestamp endTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

        System.out.println("This job started at " + startTimestamp);
        System.out.println("This job finished at: " + endTimestamp);
        System.out.println("The job took: " + (endTime - startTime)/1000000 + " milliseconds to finish");

        /* rater_id: average rating
        List<Tuple2<Integer, Double>> output = averaged.collect();
        for (Tuple2<?,?> tuple: output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */
        ctx.stop();
    }
}
