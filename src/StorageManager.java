import java.io.*;
import java.util.*;

public class StorageManager {

    private static RandomAccessFile systemCatalog;
    private static FileOutputStream outputFile;
    private static String systemCatalogName = "SystemCatalog";


    private static final int pageSize = 1024; // the number of byte we should fetch from the file per access.
    private static final int lineSizeInCatalog = 128; // the number of byte a line contains in catalog.
    private static final int maxLineInPage = 8; // number of line in a page of catalog.
    private static String lastLineInCatalog = "";

    private static HashMap<String, Integer> dictionary = new HashMap<>();

    //todo: take filenames from command line.
    //commands are kept in a different arraylist.
    public static void main(String[] args) {
        String inputFileName = args[0];
        String outputFileName = args[1];
        String dictionaryFileName = "dictionaryFile.txt";

        File file = new File(inputFileName);
        Scanner input;
        File dictionaryFile = new File(dictionaryFileName);
        Scanner inputForDictionary;

        ArrayList<String> lines = new ArrayList<>();
        ArrayList<String> commands = new ArrayList<>();
        try {
            dictionaryFile.createNewFile();
            input = new Scanner(file);
            systemCatalog = new RandomAccessFile(new File(systemCatalogName), "rw");
            getLastLineOfCatalog();
            outputFile = new FileOutputStream(new File(outputFileName), true);
            inputForDictionary = new Scanner(dictionaryFile);
            while(inputForDictionary.hasNextLine()){
                String line = inputForDictionary.nextLine();
                Scanner inputForLine = new Scanner(line);
                String typeName = inputForLine.next();
                String count = inputForLine.next();
                dictionary.put(typeName,Integer.parseInt(count));
            }
            inputForDictionary.close();

            while (input.hasNextLine()) {
                String line = input.nextLine();
                Scanner inputLine = new Scanner(line);
                String word1 = inputLine.next();
                String word2 = inputLine.next();
                word1 = word1.toLowerCase();
                word2 = word2.toLowerCase();
                commands.add(word1 + " " + word2);
                String newLine = "";
                while (inputLine.hasNext()) {
                    newLine = newLine + inputLine.next() + " ";
                }
                lines.add(newLine);
                inputLine.close();
            }
            input.close();

        } catch (IOException e) {
            e.getStackTrace();
        }

        for (int i = 0; i < commands.size(); i++) {
            switch (commands.get(i)) {
                case "create type":
                    createType(lines.get(i));
                    break;
                case "delete type":
                    deleteType(lines.get(i));
                    break;
                case "list type":
                    listTypes();
                    break;
                case "create record":
                    createRecord(lines.get(i));
                    break;
                case "delete record":
                    deleteRecord(lines.get(i));
                    break;
                case "update record":
                    updateRecord(lines.get(i));
                    break;
                case "search record":
                    searchRecord(lines.get(i));
                    break;
                case "list record":
                    listRecord(lines.get(i));
                    break;
            }
        }
        try {
            systemCatalog.close();
            outputFile.close();
            if(dictionaryFile.delete()) System.out.println("deleted successfully");
            FileOutputStream dictFile = new FileOutputStream(new File(dictionaryFileName),true);
            ArrayList<String> keys = new ArrayList<>(dictionary.keySet());
            for(int i = 0; i<keys.size();i++){
                String key = keys.get(i);
                String value = dictionary.get(keys.get(i)).toString();
                String toBeAdded = key+" "+value+"\n";
                dictFile.write(toBeAdded.getBytes());
            }

        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    //command will come without first two words.
    private static int createType(String command) {
        String typeToBeWritten = "";
        Scanner forCommand = new Scanner(command);
        String tableName = forCommand.next();
        if (tableName.length() > 9) return 0;
        if (isTypeExist(tableName)){
            return 0;
        }
        String toBeConverted = forCommand.next();
        int fieldNumber = Integer.parseInt(toBeConverted);
        if (fieldNumber > 9) return 0;
        typeToBeWritten = typeToBeWritten + tableName + " ";
        typeToBeWritten = typeToBeWritten + fieldNumber + " ";
        ArrayList<String> columnNames = new ArrayList<>();
        for (int i = 0; i < fieldNumber; i++) {
            String columnName = forCommand.next();
            if (columnName.length() > 9) return 0;
            if(columnNames.contains(columnName)) return 0;
            columnNames.add(columnName);
            typeToBeWritten = typeToBeWritten + columnName + " ";
        }
        int offset = typeToBeWritten.length();
        for (int i = 0; i < lineSizeInCatalog - offset - 1; i++) {
            typeToBeWritten = typeToBeWritten + " ";
        }
        typeToBeWritten = typeToBeWritten + "\n";
        lastLineInCatalog = typeToBeWritten;
        byte[] toBeWritten = typeToBeWritten.getBytes();
        try {
            long fileLength = systemCatalog.length();
            systemCatalog.seek(fileLength);
            systemCatalog.write(toBeWritten);
            RandomAccessFile table = new RandomAccessFile(new File(tableName + "1.txt"), "rw");
            table.close();
            forCommand.close();
        } catch (IOException e) {
            e.getStackTrace();
        }

        dictionary.put(tableName, 1);
        getLastLineOfCatalog();
        return 1;

    }

    private static int[] isRecordExist(String primaryKey, String typeName) {

        int fileCount = dictionary.get(typeName);
        int[] fileNumberOffsetPair = {-1,-1};

        try {

            for (int k = 1; k <= fileCount; k++) {
                RandomAccessFile typeFile = new RandomAccessFile(new File(typeName + k + ".txt"), "rw");
                typeFile.seek(0);
                long fileLength = typeFile.length();
                int offset = 0;
                int counterForPage = 0;
                while (fileLength > pageSize) {
                    counterForPage++;
                    byte[] bytes = new byte[pageSize];
                    typeFile.read(bytes);
                    String page = new String(bytes);
                    Scanner lineInPage = new Scanner(page);
                    for (int i = 0; i < maxLineInPage; i++) {
                        String line = lineInPage.nextLine();
                        Scanner tokensInLine = new Scanner(line);
                        String primary = tokensInLine.next();
                        if (primary.equals(primaryKey)) {
                            fileNumberOffsetPair[0]=offset;
                            fileNumberOffsetPair[1]=k;
                            return fileNumberOffsetPair;
                        }
                        offset += lineSizeInCatalog;
                    }
                    fileLength -= pageSize;
                    lineInPage.close();
                }
                offset = counterForPage * pageSize;
                byte[] remainingPart = new byte[(int) fileLength];
                typeFile.read(remainingPart);
                String remainingString = new String(remainingPart);
                int numberOfLines = (int) (fileLength / lineSizeInCatalog);
                Scanner inputForRemaining = new Scanner(remainingString);
                for (int i = 0; i < numberOfLines; i++) {
                    String line = inputForRemaining.nextLine();
                    Scanner inputForLine = new Scanner(line);
                    String primary = inputForLine.next();
                    if (primary.equals(primaryKey)) {
                        fileNumberOffsetPair[0]=offset;
                        fileNumberOffsetPair[1]=k;
                        typeFile.close();
                        return fileNumberOffsetPair;
                    }
                    offset += lineSizeInCatalog;
                }
                inputForRemaining.close();
                systemCatalog.seek(0);
                typeFile.close();
            }
        } catch (IOException e) {
            e.getStackTrace();
        }
        return fileNumberOffsetPair;
    }

    private static boolean isTypeExist(String tableName) {
        return getTypeNames().contains(tableName);
    }


    private static boolean listTypes() {
        ArrayList<String> types = getTypeNames();
        Collections.sort(types, new StringComparator());
        for (int i = 0; i < types.size(); i++) {
            try {
                outputFile.write((types.get(i) + "\n").getBytes());
            } catch (IOException e) {
                e.getStackTrace();
            }
        }
        return true;
    }


    private static ArrayList<String> getTypeNames() {
        ArrayList<String> types = new ArrayList<>();
        try {
            systemCatalog.seek(0);
            long catalogLength = systemCatalog.length();
            while (catalogLength > pageSize) {
                byte[] bytes = new byte[pageSize];
                systemCatalog.read(bytes); // first page is grabbed.
                String page = new String(bytes);
                Scanner lineInPage = new Scanner(page);
                for (int i = 0; i < maxLineInPage; i++) {
                    String line = lineInPage.nextLine();
                    Scanner tokensInLine = new Scanner(line);
                    types.add(tokensInLine.next());
                }
                catalogLength -= pageSize;
                lineInPage.close();
            }
            byte[] remainingPart = new byte[(int) catalogLength];
            systemCatalog.read(remainingPart);
            String remainingString = new String(remainingPart);
            int numberOfLines = (int) (catalogLength / lineSizeInCatalog);
            Scanner inputForRemaining = new Scanner(remainingString);
            for (int i = 0; i < numberOfLines; i++) {
                String line = inputForRemaining.nextLine();
                Scanner inputForLine = new Scanner(line);
                types.add(inputForLine.next());
            }
            inputForRemaining.close();
            systemCatalog.seek(0);
        } catch (IOException e) {
            e.getStackTrace();
        }
        return types;
    }

    private static int deleteType(String command) {
        Scanner forCommand = new Scanner(command);
        String tableName = forCommand.next();
        forCommand.close();
        if (!isTypeExist(tableName)) return 0;

        int fileCount = dictionary.get(tableName);
        for (int i = 1; i <= fileCount; i++) {
            File table = new File(tableName + i + ".txt");
            table.delete();
            if(table.delete()) System.out.println("qkewl");
        }
        dictionary.remove(tableName);
        try {
            systemCatalog.seek(0);
            long catalogLength = systemCatalog.length();
            int counterForOffset = 0;
            while (catalogLength > pageSize) {
                counterForOffset++;
                byte[] bytes = new byte[pageSize];
                systemCatalog.seek(0);
                systemCatalog.read(bytes); // first page is grabbed.
                String page = new String(bytes);
                Scanner lineInPage = new Scanner(page);
                for (int i = 0; i < maxLineInPage; i++) {
                    String line = lineInPage.nextLine();
                    Scanner tokensInLine = new Scanner(line);
                    String typeName = tokensInLine.next();
                    if (typeName.equals(tableName)) {
                        Scanner lastLine = new Scanner(lastLineInCatalog);
                        String lastName = lastLine.next();
                        if (!lastName.equals(tableName)) {
                            systemCatalog.write(lastLineInCatalog.getBytes(), (int) systemCatalog.getFilePointer(), lineSizeInCatalog);
                        }
                        systemCatalog.setLength(systemCatalog.length() - lineSizeInCatalog);
                        systemCatalog.seek(0);
                        return 0;
                    } else {
                        systemCatalog.seek(lineSizeInCatalog + systemCatalog.getFilePointer());
                    }
                }
                catalogLength -= pageSize;
                lineInPage.close();
            }
            systemCatalog.seek(counterForOffset * pageSize);
            byte[] remainingPart = new byte[(int) catalogLength];
            systemCatalog.read(remainingPart);
            String remainingString = new String(remainingPart);
            int numberOfLines = (int) (catalogLength / lineSizeInCatalog);
            Scanner inputForRemaining = new Scanner(remainingString);
            systemCatalog.seek(counterForOffset * pageSize);
            for (int i = 0; i < numberOfLines; i++) {
                String line = inputForRemaining.nextLine();
                Scanner inputForLine = new Scanner(line);
                String typeName = inputForLine.next();
                if (typeName.equals(tableName)) {
                    Scanner lastLine = new Scanner(lastLineInCatalog);
                    String lastName = lastLine.next();
                    if (!lastName.equals(tableName)) {
                        systemCatalog.write(lastLineInCatalog.getBytes(), (int) systemCatalog.getFilePointer(), lineSizeInCatalog);
                    }
                    systemCatalog.setLength(systemCatalog.length() - lineSizeInCatalog);
                    systemCatalog.seek(0);
                    return 0;
                } else {
                    systemCatalog.seek(lineSizeInCatalog + systemCatalog.getFilePointer());
                }
            }
            inputForRemaining.close();
        } catch (IOException e) {
            e.getStackTrace();
        }
        getLastLineOfCatalog();
        return 1;
    }

    private static int createRecord(String command) {
        Scanner forCommand = new Scanner(command);
        String typeName = forCommand.next();
        if (!isTypeExist(typeName)) {
            return 0;
        }

        int fileCount = dictionary.get(typeName);

        try {
            RandomAccessFile typeFile = new RandomAccessFile(new File(typeName + fileCount + ".txt"), "rw");
            ArrayList<String> fieldValues = new ArrayList<>();
            while (forCommand.hasNext()) {
                fieldValues.add(forCommand.next());
            }
            if (fieldValues.size() > 9) return 0;
            if (typeFile.length() == pageSize * 10) {
                typeFile = new RandomAccessFile(new File(typeName + (fileCount + 1) + ".txt"), "rw");
                dictionary.replace(typeName, fileCount + 1);
            }
            if (isRecordExist(fieldValues.get(0), typeName)[0] != -1){
                typeFile.close();
                return 0;
            }
            String recordToBeWritten = "";
            for (int i = 0; i < fieldValues.size(); i++) {
                recordToBeWritten += fieldValues.get(i) + " ";
            }
            int offset = lineSizeInCatalog - recordToBeWritten.length();
            for (int i = 0; i < offset - 1; i++) {
                recordToBeWritten += " ";
            }
            recordToBeWritten += "\n";
            typeFile.seek(typeFile.length());
            typeFile.write(recordToBeWritten.getBytes());
            typeFile.close();

        } catch (IOException e) {
            e.getStackTrace();
        }

        return 1;
    }

    private static int listRecord(String command) {
        ArrayList<String> allRecordsOfThatType = new ArrayList<>();
        Scanner forCommand = new Scanner(command);
        String typeName = forCommand.next();
        if (!isTypeExist(typeName)){
            return 0;
        }
        int fileNumber = dictionary.get(typeName);
        try {
            for(int k=1; k<=fileNumber; k++){
                RandomAccessFile typeFile = new RandomAccessFile(new File(typeName + k+ ".txt"), "rw");
                typeFile.seek(0);
                long fileLength = typeFile.length();
                while (fileLength > pageSize) {
                    byte[] bytes = new byte[pageSize];
                    typeFile.read(bytes); // first page is grabbed.
                    String page = new String(bytes);
                    Scanner lineInPage = new Scanner(page);
                    for (int i = 0; i < maxLineInPage; i++) {
                        String line = lineInPage.nextLine();
                        Scanner forLine = new Scanner(line);
                        String toBeAdded = "";
                        while (forLine.hasNext()) {
                            toBeAdded += forLine.next() + " ";
                        }
                        toBeAdded = toBeAdded.substring(0, toBeAdded.length() - 1);
                        toBeAdded += "\n";
                        allRecordsOfThatType.add(toBeAdded);
                    }
                    fileLength -= pageSize;
                    lineInPage.close();
                }
                byte[] remainingPart = new byte[(int) fileLength];
                typeFile.read(remainingPart);
                String remainingString = new String(remainingPart);
                int numberOfLines = (int) (fileLength / lineSizeInCatalog);
                Scanner inputForRemaining = new Scanner(remainingString);
                for (int i = 0; i < numberOfLines; i++) {
                    String line = inputForRemaining.nextLine();
                    Scanner forLine = new Scanner(line);
                    String toBeAdded = "";
                    while (forLine.hasNext()) {
                        toBeAdded += forLine.next() + " ";
                    }
                    toBeAdded = toBeAdded.substring(0, toBeAdded.length() - 1);
                    toBeAdded += "\n";
                    allRecordsOfThatType.add(toBeAdded);
                }
                inputForRemaining.close();
                typeFile.seek(0);
                typeFile.close();

            }

            sortRecords(allRecordsOfThatType);
        } catch (IOException e) {
            e.getStackTrace();
        }
        return 0;
    }

    private static int deleteRecord(String command) {
        Scanner forCommand = new Scanner(command);
        String typeName = forCommand.next();
        if (!isTypeExist(typeName)){
            return 0;
        }

        int fileCount = dictionary.get(typeName);
        String primary = forCommand.next();
        forCommand.close();
        try {
            RandomAccessFile typeFile = new RandomAccessFile(new File(typeName+fileCount+".txt"),"rw");
            typeFile.seek(typeFile.length() - lineSizeInCatalog);
            byte[] lastRecord = new byte[lineSizeInCatalog];
            typeFile.read(lastRecord);
            int[] offsetFile = isRecordExist(primary,typeName);
            int offset = offsetFile[0];
            int fileNumber = offsetFile[1];
            typeFile.close();
            if(offset==-1){
                typeFile.close();
                return 0;
            }
            typeFile = new RandomAccessFile(new File(typeName+fileNumber+".txt"),"rw");
            typeFile.seek(offset);
            typeFile.write(lastRecord);
            typeFile.seek(0);
            typeFile.close();
            typeFile = new RandomAccessFile(new File(typeName+fileCount+".txt"),"rw");
            typeFile.setLength(typeFile.length()-lineSizeInCatalog);
            typeFile.close();
            File toBeDeleted = new File(typeName+fileCount+".txt");
            if(toBeDeleted.length()==0){
                if(toBeDeleted.delete()) System.out.println("deleted successfully");
                fileCount--;
                dictionary.replace(typeName,fileCount);
            }
        } catch (IOException e) {
            e.getStackTrace();
        }
        return 0;
    }


    private static int sortRecords(ArrayList<String> records) {
        ArrayList<ArrayList<Integer>> toBeSorted = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            ArrayList<Integer> convertedToIntegers = new ArrayList<>();
            String toBeSplit = records.get(i);
            Scanner forStringToBeSplit = new Scanner(toBeSplit);
            while (forStringToBeSplit.hasNext()) {
                convertedToIntegers.add(Integer.parseInt(forStringToBeSplit.next()));
            }
            toBeSorted.add(convertedToIntegers);
            forStringToBeSplit.close();
        }

        Collections.sort(toBeSorted, new ArrayListComparator());
        for (int i = 0; i < toBeSorted.size(); i++) {
            String toBeWritten = "";
            for (int j = 0; j < toBeSorted.get(i).size(); j++) {
                toBeWritten += toBeSorted.get(i).get(j) + " ";
            }
            toBeWritten = toBeWritten.substring(0, toBeWritten.length() - 1);
            toBeWritten += "\n";
            try {
                outputFile.write(toBeWritten.getBytes());
            } catch (IOException e) {
                e.getStackTrace();
            }
        }
        return 0;
    }

    public static class StringComparator implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    public static class ArrayListComparator implements Comparator<ArrayList<Integer>> {
        @Override
        public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
            return o1.get(0).compareTo(o2.get(0));
        }
    }

    private static int updateRecord(String command) {
        Scanner forCommand = new Scanner(command);
        String typeName = forCommand.next();
        if (!isTypeExist(typeName)) {
            return 0;
        }
        ArrayList<String> fieldValues = new ArrayList<>();
        while (forCommand.hasNext()) {
            fieldValues.add(forCommand.next());
        }
        if (fieldValues.size() > 9) return 0;
        int[] offsetFile = isRecordExist(fieldValues.get(0),typeName);
        int offset = offsetFile[0];
        int fileNumber = offsetFile[1];
        if (offset == -1) return 0;
        String toBeWritten = "";
        for (int i = 0; i < fieldValues.size(); i++) {
            toBeWritten += fieldValues.get(i) + " ";
        }
        int offsetToComplete = lineSizeInCatalog - toBeWritten.length();
        for (int i = 0; i < offsetToComplete - 1; i++) {
            toBeWritten += " ";
        }
        toBeWritten += "\n";
        try {
            RandomAccessFile typeFile = new RandomAccessFile(new File(typeName + fileNumber+".txt"), "rw");
            typeFile.seek(offset);
            typeFile.write(toBeWritten.getBytes());
            typeFile.close();

        } catch (IOException e) {
            e.getStackTrace();
        }

        return 0;

    }

    private static int searchRecord(String command) {
        Scanner forCommand = new Scanner(command);
        String typeName = forCommand.next();
        if (!isTypeExist(typeName)){
            return 0;
        }
        String primary = "";
        while (forCommand.hasNext()) {
            primary = forCommand.next();
        }
        int[] offsetFile = isRecordExist(primary,typeName);
        int offset = offsetFile[0];
        int fileNumber = offsetFile[1];
        if (offset == -1) return 0;
        try {
            RandomAccessFile typeFile = new RandomAccessFile(new File(typeName + fileNumber+".txt"), "rw");
            typeFile.seek(offset);
            byte[] toBeRead = new byte[lineSizeInCatalog];
            typeFile.read(toBeRead);
            outputFile.write(toBeRead);
            typeFile.close();

        } catch (IOException e) {
            e.getStackTrace();
        }

        return 0;

    }

    private static int getLastLineOfCatalog() {
        try {
            systemCatalog.seek(systemCatalog.length() - lineSizeInCatalog);
            byte[] last = new byte[lineSizeInCatalog];
            systemCatalog.read(last);
            String temp = new String(last);
            lastLineInCatalog = temp;
            systemCatalog.seek(0);
        } catch (IOException e) {
            e.getStackTrace();
        }
        return 1;
    }
}
