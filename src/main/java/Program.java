import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.Gson;

public class Program {
    public static void main(String[] args) {
        Printer.setPrinter(System.out);

        new Program().run(args);

        System.exit(0);
    }

    public class MonitorProcess extends TimerTask {
        private OperatingSystemMXBean os;
        private Runtime runtime;
        private Papers papers;

        public MonitorProcess(Papers _papers) {
            os = ManagementFactory.getOperatingSystemMXBean();
            runtime = Runtime.getRuntime();
            papers = _papers;
        }

        @Override
        public void run() {
            String output = String.format(
                    "Memory: " + (runtime.totalMemory() - runtime.freeMemory()) +
                    "\n" +
                    "Cpu load: " + os.getSystemLoadAverage() +
                    "\n" +
                    "Lines Read: " + papers.getTotalLinesRead());

            Printer.println(output);
        }
    }

    public void run(String[] args) {
        Scanner sc = new Scanner(System.in);
        ParametersReader reader = new ParametersReader(sc);

        String keyword = reader.getKeyword();
        int tiers = reader.getTier();

        Timer monitoring = null;
        try {
            Papers papers = new Papers(reader.getFile());
            monitoring = scheduleMonitoring(1000 * 60, papers); // every minute

            Papers.PapersIterator it = papers.search(keyword, tiers);

            while (it.hasNext()) {
                Tier tier = ((Tier) it.next());

                Printer.println(">>> Tier " + tier.level);
                Printer.println(Arrays.stream(tier.papers).collect(Collectors.joining(", ")));

                // Once we are done reading this Tier set to null and call to GC to clear off the memory
                tier = null;

                Runtime.getRuntime().gc();
            }
        }
        catch (IOException e) {
            Printer.println("Error loading research papers: " + e.getMessage());
        }
        finally {
            if (monitoring != null) {
                monitoring.cancel();
            }
        }
    }

    private Timer scheduleMonitoring(int period, Papers papers) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new MonitorProcess(papers), period, period);
        return timer;
    }

    public class Papers {
        private ConcurrentHashMap<String, Integer> allPapers;
        private ConcurrentHashMap.KeySetView<String, Boolean> currentReferences;

        private Path path;
        private Gson jsonReader;

        private int totalLinesRead = 0;

        public Papers(String filePath) {
            jsonReader = new Gson();
            currentReferences  = ConcurrentHashMap.newKeySet(2000000);

            path = Path.of(filePath);
            if (filePath == null || Files.exists(path) == false || path.toString().endsWith(".txt") == false) {
                throw new IllegalArgumentException("Invalid file provided: " + filePath);
            }
        }

        public int getTotalLinesRead() {
            return totalLinesRead;
        }

        private synchronized void updateLinesRead(String id) {
            totalLinesRead += 1;
            allPapers.put(id, totalLinesRead - 1);
        }

        private void buildList(String keyword) throws IOException {
            allPapers = new ConcurrentHashMap<>();

            Instant now = Instant.now();
            Printer.info("reading file");

            getPapersStream()
                .forEach(line -> {
                    Paper paper = jsonReader.fromJson(line, Paper.class);
                    if ( keyword != null && paper.title.contains(keyword) ) {
                        currentReferences.add(paper.id);
                    }

                    updateLinesRead(paper.id);
                });

            Printer.info("done reading " + getTotalLinesRead() + " lines file in " + Duration.between(now, Instant.now()).toMillis() / 1000 + "secs");
        }

        private Stream<String> getPapersStream() throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
            return reader.lines();
            // return Files.lines(path);
        }

        public PapersIterator search(String keyword, int tier) throws IOException {
            if (allPapers == null) {
                buildList(keyword);
            }

            return new PapersIterator(allPapers, currentReferences, tier);
        }

//        public String getDetails(String id) {
//            return String.format("Id: %s, Title: %s", id, allPapers.get(id).title);
//        }

        public class PapersIterator implements Iterator {
            private int limit;
            int currIndex = 0;

            private ConcurrentHashMap.KeySetView<String, Boolean> currentReferenceSet;
            private ConcurrentHashMap<String, Integer> papers;

            private boolean foundResultsInLastSearch = true;

            public PapersIterator(ConcurrentHashMap<String, Integer> allPapers, ConcurrentHashMap.KeySetView<String, Boolean> _currentReferenceSet, int _limit) {
                limit = _limit;
                currentReferenceSet = _currentReferenceSet;
                papers = allPapers;
            }

            @Override
            public boolean hasNext() {
                return currIndex < limit && foundResultsInLastSearch;
            }

            @Override
            public Object next() {
                currIndex += 1;

                Instant now = Instant.now();
                Printer.info("analyzing tier " + currIndex);

                ConcurrentHashMap.KeySetView<String, Boolean> nextSet = ConcurrentHashMap.newKeySet();

                papers
                    .forEachEntry(100, entry -> {
                        try {
                            String id = entry.getKey();
                            Integer offset = entry.getValue();
                            Paper paper = jsonReader.fromJson(getPapersStream().skip(offset).findFirst().get(), Paper.class);

                            if (
                                    currentReferenceSet.contains(id) == true ||
                                            paper.references == null ||
                                            paper.references.length == 0
                            ) {
                                return;
                            }

                            for (String ref :
                                    paper.references) {
                                if (currentReferenceSet.contains(ref) == true)
                                    nextSet.add(entry.getKey());
                            }
                        }
                        catch (IOException e) {
                            Printer.println("Error get tier papers - terminating program " + e.getMessage());
                            System.exit(1);
                        }
                    });

                foundResultsInLastSearch = nextSet.isEmpty() == false;

                currentReferenceSet.clear();
                currentReferenceSet.addAll(nextSet);

                Printer.println("done analyzing " + currIndex + " tier in " + Duration.between(now, Instant.now()).toMillis() / 1000 + "secs");

                return new Tier(currIndex, currentReferenceSet.toArray(String[]::new));
            }
        }
    }

    public class Paper {
        public String id;
        public String title;
        public String[] references;
    }

    public class Tier {
        public int level;
        public String[] papers;

        public Tier(int _level, String[] _papers) {
            level = _level;
            papers = _papers;
        }
    }

    public static class Printer {
        private static PrintStream printTo;

        public static void setPrinter(OutputStream stream) {
            if (printTo != null) {
                printTo.close();
            }
            printTo = new PrintStream(stream);
        }

        public static void println(Object o) { printTo.println(o); }

        public static void print(Object o) { printTo.print(o); }

        public static void info(Object o) { printTo.println("[info] " + o);}
    }

    private class ParametersReader {
        private Scanner reader;
        Pattern keywordPattern;
        Pattern numberPattern;

        public ParametersReader(Scanner _sc) {
            keywordPattern = Pattern.compile("[^\\\\d\\\\s]{3,}");
            numberPattern = Pattern.compile("^[0-9]+$");
            reader = _sc;
        }

        public String getKeyword() {
            String keyword = null;
            do {
                Printer.println("Enter search keyword(no spaces; min 3 word char): ");
                keyword = reader.next();
            } while (false);//(keywordPattern.matcher(keyword).matches() == false);
            return keyword;
        }

        public int getTier() {
            String token = null;
            do {
                Printer.println("Enter an integer(tier): ");
                token = reader.next();
            } while (false);//numberPattern.matcher(token).matches() == false);
            return Integer.parseInt(token);
        }

        public String getFile() {
            return "/Users/yawarjamal/Downloads/dblp_papers_v11.txt";
        }
    }
}
