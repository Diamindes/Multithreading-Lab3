package generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InputGenerator {

  public static void generate(String outputPath, int size, int maxValue) throws IOException {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {
      Random rand = new Random();
      int[] values = new int[size];
      Arrays.fill(values, 1);
      List<String> collect = IntStream.of(values).map(operand -> operand * rand.nextInt(maxValue))
          .boxed()
          .map(Object::toString)
          .collect(Collectors.toList());

      bw.append(String.join(" ", collect));
    }
  }

}
