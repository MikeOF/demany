package demany;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    public static void main(String[] args) throws Exception {

        // first take input from standard in
        BufferedReader standardInReader = new BufferedReader(new InputStreamReader(System.in));

        // get the input string
        StringBuilder inputStringBuilder = new StringBuilder();
        String line;
        while ((line = standardInReader.readLine()) != null) {
            inputStringBuilder.append(line);
        }

        // parse the input
        Input input = new Input(inputStringBuilder.toString());

        if (input.program == Input.Program.CHECK_INDICES) {
            System.exit(checkIndices(input));

        } else if(input.program == Input.Program.DEMULTIPLEX) {
            System.exit(Demultiplex.ExecuteDemultiplex(input));

        } else {
            throw new RuntimeException("unexpected error");
        }
    }

    private static int checkIndices(Input input) {
        return 0;
    }
}
