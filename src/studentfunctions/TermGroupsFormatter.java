/**
 * This class implements the MapGroupsFunction interface to format term groups in Apache Spark.
 * It takes a key and an iterator of Tuple2<String, Integer> values and returns a Tuple2<String, Integer> result.
 */
package studentfunctions;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;

public class TermGroupsFormatter implements MapGroupsFunction<String, Tuple2<String,Integer>, Tuple2<String,Integer>>{

	@Override
	public Tuple2<String, Integer> call(String key, Iterator<Tuple2<String, Integer>> values) throws Exception {
		final int[] totalCount = {0};
		
		values.forEachRemaining(ele -> totalCount[0] += ele._2());
		
		return new Tuple2<String,Integer>(key,totalCount[0]);
	}
}

