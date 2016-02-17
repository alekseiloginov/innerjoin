package com.loginov;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs ordered inner join on two csv files and saves the result to a new csv file.
 *
 * @author Aleksei Loginov
 */
public class InnerJoin {

    public static final String COMMA = ",";
    public static final String NUMBER_FORMAT = "%09d";
    public static final int ZERO = 0;
    public static final int ONE = 1;

    public static void main(String[] args) throws IOException {

        // Asking a user for the file paths to work with
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String inputA;
        String inputB;
        String output;

        do {
            System.out.println("------------------------------------------------------------");
            System.out.println("Please, enter the FIRST valid INPUT file (*.csv) or 'exit' to quit:");
            inputA = br.readLine().trim().toLowerCase();
            if (inputA.equals("exit")) return;

            System.out.println("Please, enter the SECOND valid INPUT file (*.csv) or 'exit' to quit:");
            inputB = br.readLine().trim().toLowerCase();
            if (inputB.equals("exit")) return;

            System.out.println("Please, enter a valid OUTPUT file (*.csv) or 'exit' to quit:");
            output = br.readLine().trim().toLowerCase();
            if (output.equals("exit")) return;

        } while (!processSuccessfully(inputA, inputB, output));

        System.out.println("Inner join complete! Check out the output file.");
    }

    private static boolean processSuccessfully(String inputA, String inputB, String output) throws IOException {
        try {
            // entries intersection
            List<String> result = makeHashInnerJoin(getLines(inputA), getLines(inputB));
            writeToFile(result, output);
            return true;

        } catch (FileNotFoundException | NoSuchFileException e) {
            System.err.printf("There's no file '%s'. Please, provide a valid input file.\n", e.getMessage());
            return false;
        }
    }

    private static List<String> getLines(String filePath) throws IOException {
        return Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
    }

    private static List<String> makeHashInnerJoin(List<String> linesA, List<String> linesB) {

        List<String> result = new ArrayList<>();

        // map to associate possible values from the first column with their pairs
        Map<Integer, List<Pair<Integer, String>>> numberPairMapA = new HashMap<>();

        for (String lineA : linesA) {
            // process source A
            Pair<Integer, String> pair = createPair(lineA);

            Integer key = pair.getKey();
            // add mapping of the current pair with this pair's number
            List<Pair<Integer, String>> pairs = numberPairMapA.get(key);

            if (pairs == null) {
                pairs = new ArrayList<>();
                numberPairMapA.put(key, pairs);
            }
            pairs.add(pair);
        }

        for (String lineB : linesB) {
            // process source B
            Pair<Integer, String> pairB = createPair(lineB);

            Integer key = pairB.getKey();

            // check existence of pair mapping with the current pair's number
            List<Pair<Integer, String>> pairsA = numberPairMapA.get(key);

            if (pairsA != null) {
                for (Pair<Integer, String> pairA : pairsA) {

                    // add the necessary padding for the first value
                    String keyStr = String.format(NUMBER_FORMAT, key);

                    // if mapping is present, create a new result entry with the both values
                    result.add(keyStr + COMMA + pairA.getValue() + COMMA + pairB.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Alternative method to makeHashInnerJoin with the custom indexes.
     */
    private static List<String> makeIndexedHashInnerJoin(List<String> linesA, List<String> linesB) {

        List<String> result = new ArrayList<>();

        // array to hold one of the input sources in an ordered way
        Pair[] pairsA = new Pair[linesA.size()];

        // map to associate possible values from the first column with their indexes in the array
        Map<Integer, List<Integer>> numberIndexMapA = new HashMap<>();

        for (int i = 0; i < linesA.size(); i++) {
            // process source A
            Pair<Integer, String> pair = createPair(linesA.get(i));

            // add each entry to the array
            pairsA[i] = pair;

            Integer key = pair.getKey();
            // add mapping of the current index with this pair's number
            List<Integer> indexes = numberIndexMapA.get(key);

            if (indexes == null) {
                indexes = new ArrayList<>();
                numberIndexMapA.put(key, indexes);
            }
            indexes.add(i);
        }

        for (String lineB : linesB) {
            // process source B
            Pair<Integer, String> pairB = createPair(lineB);

            Integer key = pairB.getKey();

            // check existence of index mapping with the current pair's number
            List<Integer> indexes = numberIndexMapA.get(key);

            if (indexes != null) {
                for (Integer index : indexes) {

                    // if mapping is present, create a new result entry with the both values
                    @SuppressWarnings("unchecked")
                    Pair<Integer, String> pairA = (Pair<Integer, String>) pairsA[index];

                    // add the necessary padding for the first value
                    String keyStr = String.format(NUMBER_FORMAT, key);
                    result.add(keyStr + COMMA + pairA.getValue() + COMMA + pairB.getValue());
                }
            }
        }
        return result;
    }

    private static Pair<Integer, String> createPair(String line) {
        String[] keyValue = line.split(COMMA);
        return new Pair<>(Integer.valueOf(keyValue[ZERO]), keyValue[ONE]);
    }

    private static void writeToFile(List<String> result, String filePath) throws IOException {
        Files.write(Paths.get(filePath), result, StandardCharsets.UTF_8);
    }

}