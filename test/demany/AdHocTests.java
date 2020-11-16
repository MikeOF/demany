package demany;

import org.json.simple.ItemList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

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

    @Test
    void TestSplit() {

        Pattern pattern = Pattern.compile("(L)([0]*)([0-9]*)", Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher("L001");
        matcher.matches();

        assertEquals("1", matcher.group(3));

        matcher = pattern.matcher("L030");
        matcher.matches();

        assertEquals("30", matcher.group(3));

        matcher = pattern.matcher("L102");
        matcher.matches();

        assertEquals("102", matcher.group(3));

        matcher = pattern.matcher("L099");
        matcher.matches();

        assertEquals("99", matcher.group(3));
    }
}
