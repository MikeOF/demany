package demany;

import demany.Context.Input;
import demany.Program.Demultiplex;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;


public class Main {

    public static void main(String[] args) throws Exception {

        // first set the global logger
        setupStandardOutLogging();

        // take input from standard in
        BufferedReader standardInReader = new BufferedReader(new InputStreamReader(System.in));

        StringBuilder inputStringBuilder = new StringBuilder(); String line;
        while ((line = standardInReader.readLine()) != null) { inputStringBuilder.append(line); }

        // parse the input
        Input input = new Input(inputStringBuilder.toString());

        // dispatch to the demultiplex program
        System.exit(Demultiplex.ExecuteDemultiplex(input));
    }

    private static void setupStandardOutLogging() {

        // clear any handlers
        Logger globalLogger = Logger.getGlobal();
        for (Handler handler : globalLogger.getHandlers()) { globalLogger.removeHandler(handler); }

        // set the standard output handler
        SimpleFormatter fmt = new SimpleFormatter();
        StreamHandler sh = new StreamHandler(System.out, fmt);
        globalLogger.addHandler(sh);

        // set the format
        System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp %2$s %4$s: %5$s%n"
        );
    }
}
