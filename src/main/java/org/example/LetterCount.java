package org.example;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
public class LetterCount {
    public static void main(String[] args) throws Exception {
        String input = null;
        String output = null;
        //本地调试时将上面两行改成本地地址，然后把下面的try-catch注释掉
        ParameterTool params = ParameterTool.fromArgs(args);
        try {
            input = params.getRequired("input");
            output = params.getRequired("output");
        } catch (RuntimeException e) {
            System.out.println("Argument Error");
            e.printStackTrace();
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<String> text = env.readTextFile(input);
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        env.execute("Flink Batch Java API Skeleton");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    String[] letters = token.split("");
                    //加一层循环，把单词打成字母
                    for (String letter : letters) {
                        if(letter.length()>0) {
                            System.out.println(letter);
                            out.collect(new Tuple2<String, Integer>(letter, 1));
                        }
                    }
                }
            }
        }
    }
}


