import java.util.regex.Pattern
import java.util.regex.Matcher

/**
* A simple and light-weight builder-like class to help with migration sources IO.
* Provides an ordered source transformation (in insertion order), plain line, regex-filtered line processing.
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
			if (params.localDirPath != null) {
				this.localDirPath = params.localDirPath
				LOGGER.info "Local dir set to: $localDirPath"
			}

			if (sourceDirPath != null) {
				this.sourceDirPath = params.sourceDirPath
				LOGGER.info "Source dir set to: $sourceDirPath"
			}

			if (params.skippingAllowed != null && params.skippingAllowed) {
			    skippedLinesFileWriter = getFile('skippedTransform.log', false).newWriter()
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

	public File getFile(String filename) {
		return getFile(filename, false)
	}


	public File getFile(String filename, boolean deleteIfExists) {
	    File file = new File(localDirPath, filename) //creates new files locally, which is good
	    if (deleteIfExists && file.exists()) {
	        file.delete()
	    }
	    return file
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

	public String transform(String infile, String outfile, String pattern, Closure transformer, boolean merge) {
	    LOGGER.info "Transforming $infile ${outfile != null ? 'to ' + outfile : ''}"
		
		def writer = new NoOpWriter()
		if (outfile != null) {
		    writer = getFile(outfile, !merge).newWriter(true)
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
	    
	    if (pattern != null) {
	        def compiled = Pattern.compile(pattern)
	        new File(infile).eachLine{line ->
	            def mtchr = compiled.matcher(line)
	            if (mtchr.find()) {
	            	def transformerParams = transformer.getParameterTypes()
	            	if (transformerParams.length == 1 && Matcher.class.isAssignableFrom(transformerParams[0])) {
	                	printOrReturn transformer(mtchr)
	            	}
	            	else {
	            		printOrReturn transformer((1..mtchr.groupCount()).collect {idx -> mtchr.group(idx)})
	            	}
	                i++
	            }
	            else {
	                if (skippedLinesFileWriter != null) {
	                    skippedLinesFileWriter.println "$infile\n$line"
	                }
	                else {
	                    LOGGER.error "Incorrect file format\n/${pattern.toString()}/\n$line"
	                    terminate "Processing $infile interrupted."
	                }
	            }
	        }
	    }
	    else if (transformer != null) {
	        new File(infile).eachLine{line ->
	            printOrReturn transformer(line)
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

	public void upload(Closure uploadClosure) {
		boolean merge = false
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
		        ,merge
		    )
			
			if (source.onMatchClosure != null) {
				printTimePassed("Transform of $realFile finished.")
			}
		    
		    merge = source.tableName == null
		    if (!merge) {
		        uploadClosure.call(source.tableName
		            ,transformedInput
		        )
		    }
		    else {
		        LOGGER.info "File will be merged"
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
	String linePattern
	Closure onMatchClosure
	String tableName
	Closure doLastClosure
	
	public void inputFile(String inputFile) {
		this.inputFile = inputFile
	}

	public void outputFile(String outputFile) {
		this.outputFile = outputFile
	}
	
	public void linePattern(String linePattern) {
		this.linePattern = linePattern
	}

	public void tableName(String tableName) {
		this.tableName = tableName
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
linePattern: $linePattern
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