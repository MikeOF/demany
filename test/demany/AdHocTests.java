package demany;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.stream.Collectors;

public class AdHocTests {

    @Test
    void TestSort() {

        HashMap<String, LinkedList<Integer>> map = new HashMap<>();

        map.put("a", new LinkedList<>());
        map.get("a").add(1);
        map.get("a").add(2);
        map.get("a").add(3);

        map.put("b", new LinkedList<>());
        map.get("b").add(1);
        map.get("b").add(2);

        map.put("c", new LinkedList<>());
        map.get("c").add(1);
        map.get("c").add(2);
        map.get("c").add(3);
        map.get("c").add(4);

        LinkedList<String> expectedList = new LinkedList<String>(Arrays.asList("c", "a", "b"));
        assertEquals(expectedList, map.keySet().stream().sorted(Comparator.comparingInt(k -> -map.get(k).size())).collect(Collectors.toList()));
    }
}
