package com.loginov;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs indexed inner join on two large csv files and saves the result to a new csv file.
 *
 * @author Aleksei Loginov
 */
public class IndexedInnerJoin {

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
            Map<Integer, List<Long>> indexMap = getIndexMap(inputA);
            List<String> result = makeInnerJoin(inputA, inputB, indexMap);
            Files.write(Paths.get(output), result, StandardCharsets.UTF_8);
            return true;

        } catch (FileNotFoundException | NoSuchFileException fnf) {
            System.err.printf("There's no file '%s'. Please, provide a valid input file.\n", fnf.getMessage());
            return false;
        }
    }

    private static Map<Integer, List<Long>> getIndexMap(String filePath) throws IOException {
        Map<Integer, List<Long>> indexMap = new HashMap<>();

        try (RandomAccessFile rafA = new RandomAccessFile(filePath, "r")) {

            long filePointer = rafA.getFilePointer();
            for (String line = rafA.readLine(); line != null; filePointer = rafA.getFilePointer(), line = rafA.readLine()) {
                String[] keyValue = line.split(COMMA);
                Integer key = Integer.valueOf(keyValue[ZERO]);

                List<Long> pointers = indexMap.get(key);
                if (pointers == null) {
                    pointers = new ArrayList<>();
                    indexMap.put(key, pointers);
                }
                // associate values from the first column with file pointers
                pointers.add(filePointer);
            }
        }
        return indexMap;
    }

    private static List<String> makeInnerJoin(String pathA, String pathB, Map<Integer, List<Long>> indexMap) throws IOException {
        List<String> result = new ArrayList<>();

        try (RandomAccessFile rafA = new RandomAccessFile(pathA, "r"); RandomAccessFile rafB = new RandomAccessFile(pathB, "r")) {

            for (String lineB = rafB.readLine(); lineB != null; lineB = rafB.readLine()) {
                String[] keyValueB = lineB.split(COMMA);
                List<Long> pointers = indexMap.get(Integer.valueOf(keyValueB[ZERO]));

                if (pointers != null) {
                    for (Long pointer : pointers) {
                        // use file pointers to jump to the beginning of the right entry
                        rafA.seek(pointer);
                        String lineA = rafA.readLine();

                        if (lineA != null) {
                            String[] keyValueA = lineA.split(COMMA);
                            // add the necessary padding for the first value
                            String keyStr = String.format(NUMBER_FORMAT, Integer.valueOf(keyValueA[ZERO]));

                            result.add(keyStr + COMMA + keyValueA[ONE] + COMMA + keyValueB[ONE]);
                        }
                    }
                }
            }
        }
        return result;
    }
}
