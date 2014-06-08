package nz.govt.moh.hce
import java.util.regex.Pattern
import java.util.regex.Matcher

/**
* A simple and light-weight builder-like class to help with migration sources IO.
* Provides an ordered source transformation (in insertion order), plain/regex-filtered line processing.
* Throws TerminatedException if there is a problem 
*
* author: Alexander Semelit
*/
class Migrator {
	def LOGGER
	List<SourceDescriptor> sources = []
	double started = new Date().getTime()
	String localDirPath = "."
	String sourceDirPath = "."
	String separator
	Writer skippedLinesFileWriter
	
	public Migrator() {
		this(null)
	}

	public Migrator(Map<?, ?> params) {
		if (params == null || params.logger == null) {
			this.LOGGER = new StdOutLogger()
		}
		else {
			this.LOGGER = params.logger
		}

		if (params != null) {
			if (params.sourceDirPath != null) {
				this.sourceDirPath = params.sourceDirPath
				LOGGER.info "Source dir set to: $sourceDirPath"
			}

			if (params.skippingAllowed != null && params.skippingAllowed) {
			    skippedLinesFileWriter = new File('skippedTransform.log').newWriter()
			}

			if (params.separator != null) {
				this.separator = params.separator
				LOGGER.info "Column separator set to $separator"
			}
		}
	}

	public Migrator source(Closure sourceConfig) {
		SourceDescriptor newSource = new SourceDescriptor()
		sources.add(newSource)
		sourceConfig.setDelegate(newSource)
		sourceConfig.call()
		this
	}

	public void printTimePassed(String message) { 
	    double finished = new Date().getTime()
	    LOGGER.info "$message Elapsed: ${(finished - started)/1000D}s"
	}

	public void terminate(String message) {
	    printTimePassed("")
	    throw new TerminatedException(message)    
	}

	public String findFile(String filename) {
		return findFile(filename, false)
	}
	
	public String findFile(String filename, boolean shouldExist) {
		LOGGER.info "Looking for $filename in $sourceDirPath"
	    def results = new File(sourceDirPath).listFiles({dir, name -> name ==~ "$filename"} as FilenameFilter)
		
	    if (results != null && results.length == 1) {
	        return results[0].getCanonicalPath()
	    }
	    else if (!localDirPath.equals(sourceDirPath)) {
			LOGGER.info "Looking for $filename in $localDirPath"
			results = new File(localDirPath).listFiles({dir, name -> name ==~ "$filename"} as FilenameFilter)
			if 	(results != null && results.length == 1) {
				return results[0].getCanonicalPath()
			}
	    }
		
		if (shouldExist) {
			terminate "Error getting file for /$filename/: $results"
		}
		else {
			LOGGER.debug "File not found or there is more than one matching /$filename/: $results"
			return null
		}
	}

	public String transform(String infile, String outfile, Pattern pattern, Closure onMatchClosure, Closure onInvalidClosure, boolean append) {
	    LOGGER.info "Transforming $infile ${outfile != null ? 'to ' + outfile : ''}"
		
		def writer = new NoOpWriter()
		if (outfile != null) {
		    writer = new File(outfile).newWriter(append)
		}
		
	    long i = 0;
	    def printOrReturn = { newLine ->
	        if (newLine != null) {
	            writer.println newLine
	        }
	        else {
	            i--
	        }
	    }
	    
	    boolean skippedLinesFileNamePrinted = false

	    if (pattern != null) {
	        new File(infile).eachLine{line ->
	            Matcher mtchr = pattern.matcher(line)
	            if (mtchr.find()) {
	            	def onMatchClosureParams = onMatchClosure.getParameterTypes()
	            	if (onMatchClosureParams.length == 1 && Matcher.class.isAssignableFrom(onMatchClosureParams[0])) {
	                	printOrReturn onMatchClosure(mtchr)
	            	}
	            	else {
	            		if (mtchr.groupCount() > 1) {
	            			printOrReturn onMatchClosure((1..mtchr.groupCount()).collect {idx -> mtchr.group(idx)} as List<String>)
	            		}
	            		else if (mtchr.groupCount() == 1) {
	            			printOrReturn onMatchClosure(mtchr.group(1))
	            		}
	            		else {
	            			printOrReturn onMatchClosure(line)
	            		}
	            	}
	                i++
	            }
	            else {
	            	if (skippedLinesFileWriter != null) {
	                	if (!skippedLinesFileNamePrinted) {
	                		skippedLinesFileWriter.println "$infile"
	                		skippedLinesFileNamePrinted = true
	                	}
	                    skippedLinesFileWriter.println "$i : $line"
	                }    
	            	
	            	if (onInvalidClosure != null) {
	            		onInvalidClosure.call(infile, i, mtchr)
	            	}	            
	                else {
	                    LOGGER.error "Incorrect file format\n/${pattern.toString()}/\n$line"
	                    terminate "Processing $infile interrupted."
	                }
	            }
	        }
	    }
	    else if (onMatchClosure != null) {
	        new File(infile).eachLine{ line ->
	            printOrReturn onMatchClosure(separator != null ? line.tokenize(separator) : line)
	            i++
	        }
	    }
	    else {
	        writer.close()
	        LOGGER.info "Done. No need to transform"
	        return infile
	    }
	    
	    writer.close()
	    LOGGER.info "Done. Total transformed $i"
	    return outfile
	}

	public void uploadAll(Closure uploadAllClosure) {
		process(null, uploadAllClosure)
	}

	public void uploadEach(Closure uploadEachClosure) {
		process(uploadEachClosure, null)
	}

	public void process() {
		process(null, null)
	}

	public void process(Closure uploadEachClosure, Closure uploadAllClosure) {
		sources.each { source ->
		    if (source.inputFile == null) {
		        LOGGER.error "Source should have an inputFile!\n$source"
		        terminate "Migration interrupted."
		    }

		    String realFile = findFile(source.inputFile, true)
		    String transformedInput = source.onMatchClosure == null ? realFile : transform(realFile
		        ,source.outputFile
		        ,source.linePattern
		        ,source.onMatchClosure
		        ,source.onInvalidClosure
		        ,source.appendOutput
		    )
			
			if (source.onMatchClosure != null) {
				printTimePassed("Transform of $realFile finished.")
			}
		    
		    if (uploadEachClosure != null) {
			    if (source.tableName != null) {
			        uploadEachClosure.call(source.tableName
			            ,transformedInput
			        )
			    }
			    else {
			        LOGGER.info "Skipping upload"
			    }
			}

		    Closure callback = source.doLastClosure
		    if (callback != null) {
		        LOGGER.info "Triggering callback function for $source.inputFile"
		        callback.call()
		        printTimePassed("Callback for $realFile finished.")
		    }
		}

		if (skippedLinesFileWriter != null) {
			skippedLinesFileWriter.close()
		}

		if (uploadAllClosure != null) {
			uploadAllClosure.call(sources)
		}

	}

	private class NoOpWriter {
		void println(def obj) {
			//ignoring
		}
		
		void close() {
			//ignoring
		}
	}
	
	private class StdOutLogger {
		void info(String msg) {
			println "INFO: $msg"
		}

		void error(String msg) {
			println "ERROR: $msg"
		}

		void warn(String msg) {
			println "WARN: $msg"
		}

		void debug(String msg) {
			println "DEBUG: $msg"
		}

	}
}

class SourceDescriptor {
	String inputFile
	String outputFile
	Pattern linePattern
	boolean appendOutput = false
	Closure onMatchClosure
	Closure onInvalidClosure
	String tableName
	Closure doLastClosure

	public void appendOutput(boolean appendOutput) {
		this.appendOutput = appendOutput
	}
	
	public void inputFile(String inputFile) {
		this.inputFile = inputFile
	}

	public void outputFile(String outputFile) {
		this.outputFile = outputFile
	}
	
	public void linePattern(def linePattern) {
		switch (linePattern) {
			case Pattern:
				this.linePattern = linePattern
				break
			default:
				this.linePattern = Pattern.compile(linePattern)
		}
		
	}

	public void tableName(String tableName) {
		this.tableName = tableName
	}	

	public void onInvalid(Closure onInvalidClosure) {
		onInvalidClosure.setDelegate(this)
		this.onInvalidClosure = onInvalidClosure
	}
	
	public void onMatch(Closure onMatchClosure) {
		onMatchClosure.setDelegate(this)
		this.onMatchClosure = onMatchClosure
	}
	
	public void doLast(Closure doLastClosure) {
		doLastClosure.setDelegate(this)
		this.doLastClosure = doLastClosure
	}

	public String toString() {
		return """inputFile: $inputFile
outputFile: $outputFile
appendOutput: $appendOutput
linePattern: ${linePattern != null ? linePattern.toString() : linePattern}
onMatch: ${onMatchClosure != null ? 'specified' : 'not specified'}
tableName: $tableName
doLast: ${doLastClosure != null ? 'specified' : 'not specified'}"""
	}
}

class TerminatedException extends RuntimeException {
	public TerminatedException(String msg) {
		super(msg)
	}
}