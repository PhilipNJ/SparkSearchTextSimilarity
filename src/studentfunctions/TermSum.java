package studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;
import scala.Tuple2;

/**
 * A custom implementation of the ReduceFunction interface for summing the values of Tuple2<String, Integer> objects.
 */
public class TermSum implements ReduceFunction<Tuple2<String,Integer>> {
    private static final long serialVersionUID = -4931804781L;

    /**
     * Sums the values of two Tuple2<String, Integer> objects.
     *
     * @param t1 the first Tuple2<String, Integer> object
     * @param t2 the second Tuple2<String, Integer> object
     * @return a new Tuple2<String, Integer> object with the summed values
     * @throws Exception if an error occurs during the summing process
     */
    @Override
    public Tuple2<String, Integer> call(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
        String key = t1._1;
        int sum = t1._2 + t2._2;
        return new Tuple2<>(key, sum);
    }
}
