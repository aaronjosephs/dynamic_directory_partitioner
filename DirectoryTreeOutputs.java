import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DirectoryTreeOutputs<KEY,VALUE> extends MultipleOutputs<KEY,VALUE> {
	
	private static int [] directoryColumnNumbers;
    private static String [] directoryColumnPatterns;
    private static String [] directoryColumnReplacements;
	private static String DELIMITER;
	/*
	 * default delimiter is ','
	 */

    static class InvalidPatternsException extends Exception {
        public InvalidPatternsException (String message) {
            super(message);
        }
    }
    /*
    In some cases the regular expressions given will not be valid to use
     */

	public DirectoryTreeOutputs(Context context, int [] dcn)
    throws InvalidPatternsException {
        this (context, dcn,null,null,",");
	}

    public DirectoryTreeOutputs(Context context, int [] dcn, String [] dcp, String [] dcr)
    throws InvalidPatternsException{
        this (context,dcn,dcp,dcr,",");
    }

	public DirectoryTreeOutputs(Context context, int [] dcn, String [] dcp, String [] dcr, String dlm)
    throws InvalidPatternsException{
		super(context);
		directoryColumnNumbers = dcn;
        directoryColumnPatterns = dcp;
        directoryColumnReplacements = dcr;
		DELIMITER = dlm;
        if (directoryColumnPatterns != null) {
            if (directoryColumnPatterns.length != directoryColumnNumbers.length || directoryColumnReplacements.length != directoryColumnNumbers.length) {
                throw new InvalidPatternsException("Array sizes don't match");
            }
        }
	}
	
    public static String rowToDirectory (String s) {
        if (directoryColumnPatterns == null || directoryColumnReplacements == null) {
            return rowToDirectoryWithIndicies(s);
        }
        else {
            return rowToDirectoryWithTransform(s);
        }
    }

    public static String rowToDirectoryWithIndicies (String s) {
        String [] rowArray = s.split(DELIMITER);
        StringBuilder directoryName = new StringBuilder();
        for (int i : directoryColumnNumbers) {
            if (rowArray[i] == "") {
                directoryName.append("EMPTY/");
                //empty field can't have no directory name
            }
            else {
                directoryName.append(rowArray[i]);
                directoryName.append("/");
            }
        }
        return directoryName.toString();
    }

    //at this point I don't really like the way in which transformations to the directory structure
    // are handled but I can't think of another way that is as versatile
    public static String rowToDirectoryWithTransform(String s) {
        String [] rowArray = s.split(DELIMITER);
        StringBuilder directoryName = new StringBuilder();
        for (int i = 0; i < directoryColumnNumbers.length;++i) {
            int index = directoryColumnNumbers[i];
            String column = rowArray[index];
            String pattern = directoryColumnPatterns[i];
            String replacement = directoryColumnReplacements[i];

            if (column == "") {
                directoryName.append("EMPTY");
            }
            else if (pattern == null || replacement == null) {
            //no transformation will be done on this columns name use the normal approach
                directoryName.append(column);
            }
            else {
                String transformed = column.replaceFirst(pattern, replacement);
                /*
                validate transformed string since pretty much relying on user to get it right
                 */
                assert (!transformed.startsWith("/") && !transformed.endsWith("/"));
                //check that string doesn't start or end with '/', should probably throw an exception for it at some point
                //other than that the user is free to manipulate the directory path anyway they want
                directoryName.append(transformed);
            }
            directoryName.append("/");
        }
        return directoryName.toString();
    }

    public void writeWithKey (KEY key, VALUE value) throws IOException, InterruptedException {
        StringBuilder dir = new StringBuilder();
        dir.append(key.toString());
        dir.append(rowToDirectory(value.toString()));
        super.write(key,value,dir.toString());
    }

    public void write (KEY key, VALUE value) throws IOException, InterruptedException {
    	super.write(key,value,rowToDirectory(value.toString()));
    }

    public void writeWithName (String namedOutput, KEY key, VALUE value) throws IOException, InterruptedException {
        super.write(namedOutput,key,value,rowToDirectory(value.toString()));
    }
}