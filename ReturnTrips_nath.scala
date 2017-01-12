import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(trips : Dataset[Row], spark : SparkSession) : Dataset[Row] = {
    import spark.implicits._

        /* GENERAL
        This functions matches trips with return trips.
        Given a trip a, we consider another trip b as a return trip iff::
            i) b’s pickup time is within 8 hours after a’s dropoff time
            ii) b’s pickup location is within 100m of a’s dropoff location
            iii) b’s dropoff location is within 100m of a’s pickup location */

        /* Naive Idea:
        Join all trips pairwise and verify these conditions for every pair.
        Problem: 
        The dataset size would explode and it would not be computationally feasible. */

        /* Better Idea:
        Instead of joining all trips pairwise, group them into (smartly chosen) baskets.
        This will reduce the joined table size by a large amount.
        Ideally matching trips are located in the same basket.
        
        We will to create such baskets based on location and on time */

        // Radius of the earth
        val R = 6371000
        val pi = math.Pi
        
        // Formula for exact distance computation via the great circle distance 
        def makeDistanceExpression(lat1 : Column, lat2 : Column,
                                   long1 : Column, long2 : Column) : Column = {
            val dLat = toRadians(lat1 - lat2)
            val dLon = toRadians(long1 - long2)
            val cterm = (pow(sin(dLat/2),2) +
                         pow(sin(dLon/2),2) *
                         cos(toRadians(lat1)) *
                         cos(toRadians(lat2)))
            val deltasigma = lit(2) * asin(sqrt(cterm))
            return deltasigma*lit(6371e3)
        }

        /* First create those location-based baskets.
        They can be imagined as putting a grid of squares over the earth, with
        sides of length 100m.
        For reasons of spherical geometry, those squares will not be exact, but
        the side-length is always > 100m. */

        // Latitude "base-angle": for every "angle_lat" degrees we made >100m distance
        val angle_lat = (2*pi*R)/(101*360)
        // max latitude found in our dataset
        val max_lat = 60.908756256113516
        // Longitude "base-angle": for every "angle_lon" degrees we made >100m distance
        val angle_lon = (2*pi*R*math.cos(math.toRadians(max_lat)))/(101*360)

        def bucket_lat(lat: Column) : Column = {
            val lat_bucket = floor(lat * lit(angle_lat))
            return lat_bucket 
        }

        def bucket_long(long: Column) : Column = {   
            val long_bucket = floor(long * lit(angle_lon))
            return long_bucket
        }

        
        // Time based baskets will round dropoff and pickup time to 8h groups
        def bucket_time(time: Column) : Column = {   
            val time_bucket = floor((time.cast("long") / 29800D)) // 28800D
            return time_bucket
        }

        // Drop all columns that are never used.
        val trips2 = trips.
            drop($"VendorID").
            drop($"passenger_count").
            drop($"trip_distance").
            drop($"RatecodeID").
            drop($"store_and_fwd_flag").
            drop($"payment_type").
            drop($"fare_amount").
            drop($"extra").
            drop($"mta_tax").
            drop($"tolls_amount").
            drop($"improvement_surcharge").
            drop($"total_amount").
            drop($"tip_amount")

        // Add the Bucket Columns to our Dataset
        val trips_bucketed = trips2.
            withColumn("Pickup_Long_Bucket", bucket_long($"pickup_longitude")).
            withColumn("Pickup_Lat_Bucket", bucket_lat($"pickup_latitude")).
            withColumn("Dropoff_Long_Bucket", bucket_long($"dropoff_longitude")).
            withColumn("Dropoff_Lat_Bucket", bucket_lat($"dropoff_latitude")).
            withColumn("Pickup_Time_Bucket", bucket_time($"tpep_pickup_datetime")).
            withColumn("Dropoff_Time_Bucket", bucket_time($"tpep_dropoff_datetime"))

        /* We need to look at neighbouring buckets too to get all potential
        matches, therefore we will introduce clones of each trip, with adjusted
        Pickup-Location buckets.
        
        This makes sure that trip a and trip b get matched even if the dropoff
        of trip b is in a neighbour-square to the pickup of trip a (and analogue
        for for a.dropoff and b.pickup)

        As we always want to match dropoff to pickup (and vice-versa) it is
        sufficient to clone one of those and not both (cloning both would
        actually introduce duplicates) */
        val trips_cloned = trips_bucketed.
            withColumn(
                "Pickup_Long_Bucket", 
                explode(array($"Pickup_Long_Bucket"-1,
                              $"Pickup_Long_Bucket",
                              $"Pickup_Long_Bucket"+1))).
            withColumn(
                "Pickup_Lat_Bucket", 
                explode(array($"Pickup_Lat_Bucket"-1,
                              $"Pickup_Lat_Bucket",
                              $"Pickup_Lat_Bucket"+1)))

        /* We will create a seperate value the time-clones, as we only need
        them on one side of the join */
        val trips_cloned_with_time = trips_cloned.
            withColumn(
                "Pickup_Time_Bucket", 
                explode(array($"Pickup_Time_Bucket"-1,
                              $"Pickup_Time_Bucket")))


        // Perform the join based on the introduced buckets!
        val trips_joined = trips_cloned.as("to").
            join(
                trips_cloned_with_time.as("back"), 
                $"to.Pickup_Long_Bucket" === $"back.Dropoff_Long_Bucket" &&
                $"to.Pickup_Lat_Bucket" === $"back.Dropoff_Lat_Bucket" &&
                $"back.Pickup_Long_Bucket" === $"to.Dropoff_Long_Bucket" &&
                $"back.Pickup_Lat_Bucket" === $"to.Dropoff_Lat_Bucket" &&
                $"to.Dropoff_Time_Bucket" === $"back.Pickup_Time_Bucket", "inner")
        
        // Drop buckets as they will not be used anymore.
        val trips_joined2 = trips_joined.
            drop($"to.Pickup_Long_Bucket").
            drop($"to.Pickup_Lat_Bucket").
            drop($"to.Dropoff_Long_Bucket").
            drop($"to.Dropoff_Lat_Bucket").
            drop($"back.Pickup_Long_Bucket").
            drop($"back.Pickup_Lat_Bucket").
            drop($"back.Dropoff_Long_Bucket").
            drop($"back.Dropoff_Lat_Bucket").
            drop($"to.Pickup_Time_Bucket").
            drop($"to.Dropoff_Time_Bucket").
            drop($"back.Pickup_Time_Bucket").
            drop($"back.Dropoff_Time_Bucket")

        // Finally we will "really" check our initial conditions.
        // Begin with the time-restriction (see i) above).
        val trips_matched = trips_joined2.
            where((($"to.tpep_dropoff_datetime").cast("long")) <
                  (($"back.tpep_pickup_datetime").cast("long"))).
            where((($"to.tpep_dropoff_datetime").cast("long")) + 28800D >
                  (($"back.tpep_pickup_datetime").cast("long")))

        // Now check the location-restrictions (see ii) and iii) above)
        val trips_matched_final = trips_matched.
            where(makeDistanceExpression(
                $"back.pickup_latitude",
                $"to.dropoff_latitude",
                $"back.pickup_longitude",
                $"to.dropoff_longitude") <= 100).
            where(makeDistanceExpression(
                $"to.pickup_latitude",
                $"back.dropoff_latitude",
                $"to.pickup_longitude",
                $"back.dropoff_longitude") <= 100)

        return trips_matched_final
    }
}
