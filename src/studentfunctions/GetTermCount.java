package studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;

public class GetTermCount implements MapFunction<Tuple2<String,Integer>,String>{

	@Override
	public String call(Tuple2<String, Integer> value) throws Exception {
		// TODO Auto-generated method stub
		return value._1();
	}
	
}
