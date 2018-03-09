import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionalProgramingExample {
    public static void main(String[] args) {
        List<String> names = Arrays.asList("Mary", "Isla", "Sam");
        names.forEach(a -> System.out.println(a.toUpperCase()));
        Collections.sort(names, (a, b) -> b.compareTo(a));
        names
            .stream()
            .filter(s -> s.startsWith("m"))
            .map(String::toUpperCase)
            .sorted()
            .forEach(System.out::println);

        Map<Integer, String> myMap = new HashMap<>();
        myMap.put(1, "Charles");

    }
}
