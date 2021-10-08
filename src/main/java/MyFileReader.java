import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class MyFileReader {
    List<String> labeledIds = new ArrayList<>();
    Path path = Paths.get("dataset\\dataset");

    // Finds all the files with the provided file extension and returns their paths
    public List<Path> findByFileExtension(Path path, String fileExtension) throws IOException {
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("Path must be a directory!");
        }
        List<Path> result;
        try (Stream<Path> walk = Files.walk(path)) {
            result = walk
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(fileExtension))
                    .toList();
        }
        return result;
    }

    //Finds the user directories that have a numeric name
    public List<Path> findUserDirectories(Path path) throws IOException {
        List<Path> result;
        try (Stream<Path> walk = Files.walk(path)) {
            result = walk.filter(Files::isDirectory)
                    .filter(path1 -> path1.getFileName().toString().matches("-?\\d+"))
                    .toList();
        }
        return result;
    }

    //Gets the IDs of the users with labeled activities
    public void getLabelIds() {
        try (Stream<Path> pathStream = Files.find(path,
                1,
                (p, basicFileAttributes) ->
                    p.getFileName().toString().equalsIgnoreCase("labeled_ids.txt"))) {
            List<Path> p = pathStream.toList();
            BufferedReader br = new BufferedReader(new FileReader(p.get(0).toString()));
            String line;
            while ((line = br.readLine()) != null) {
                labeledIds.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads the labels.txt files at the provided path and returns a list containing all the rows of the document
     * @param path the path where the labels.txt file is found
     * @return a list containing the rows of the document
     */
    public List<String> getLabelsTxt(Path path) {
        List<String> labels = new ArrayList<>();
        try (Stream<Path> pathStream = Files.find(path,
                1,
                (p, basicFileAttributes) ->
                    p.getFileName().toString().equalsIgnoreCase("labels.txt"))) {
            List<Path> paths = pathStream.toList();
            BufferedReader br = new BufferedReader(new FileReader(paths.get(0).toString()));
            String line;
            br.readLine(); //skips the first line in the document
            while ((line = br.readLine()) != null) {
                labels.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return labels;
    }


}
