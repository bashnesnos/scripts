import java.util.regex.Pattern
import java.util.regex.Matcher
import java.util.Date
import java.text.SimpleDateFormat
import java.text.ParseException

double started = new Date().getTime()
def sT = "|" //printable field terminator
def rT = "\\|" //searchable(regex eligible) field terminator
def server = "localhost"
def dbname = "Yodb"
def rainbow = new HashMap<String, String>(1 << 24)
def sourceDir = "."

def cli = new CliBuilder(usage:'migrate.groovy [options] [server dbname]')
cli.b('Just outputs commands(so you can make a batch file)')
cli.i('Ignore incorrect lines(dump them to the skippedTransform.log)')
cli.p(args:1, argName:'path', 'Path to a directory with the extracts')
def options = cli.parse(args)
if (options) {
    def scriptArgs = options.arguments()
    if ( scriptArgs != null && scriptArgs.size() == 2) {
        server = scriptArgs[0]
        System.err.println "Set server to: $server"
        dbname = scriptArgs[1]
        System.err.println "Set db to: $dbname"
    }
}

if (options.p) {
    sourceDir = options.p
}

//Utility
def months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def migrator = new Migrator([sourceDirPath: sourceDir, skippingAllowed: options.i])

def batchFileWriter 
if (options.b) {
    batchFileWriter = migrator.getFile('migrate.bat', true).newWriter()
}

long entryId = 1

def entryTimeFormat = '%s-%02d-%02d'
def toDateString = { format, year, month, day, optFlag ->
    if (year != null && month != null && day != null) {
        if (optFlag == null) {
            return String.format(format, year, month =~ /[a-zA-Z]/ ? months.indexOf(month) + 1 : Integer.valueOf(month), Integer.valueOf(day))
        }
        else {
            return String.format(format, year, optFlag.equals("M") ? 0 : Integer.valueOf(month), optFlag ==~ /[MD]/ ? 0 : Integer.valueOf(day))
        }
    }
    else {
        return ""
    }
}

def execAndWait = {command ->
    if (!options.b) { 
        def proc = command.execute()
        def out = new StringBuffer()
        def err = new StringBuffer()
        proc.consumeProcessOutput(out, err)
        proc.waitFor()
        def outStr = out.toString()
        def errStr = err.toString()
        migrator.LOGGER.info outStr
        migrator.LOGGER.info errStr
        if (outStr.contains('Error') || errStr.contains('Error')) {
            println "Got error during execution"
            if (!options.i) {
                migrator.LOGGER.error "Nested process got error. Terminating"
                migrator.terminate "Processing $command interrupted."
            }
        }
    }
    else {
        batchFileWriter.println "$command".replace("$sT", "\"$sT\"")
    } 
}

def getFileNameFromPath = { path ->
    return path.lastIndexOf('\\') > -1 ? path.substring(path.lastIndexOf('\\') + 1) : path
}

def makeFormatFile = { table, inFile ->
    def frmtFile = "${getFileNameFromPath(inFile).replace('.bcp','')}Format.fmt"
    migrator.LOGGER.info "Creating $frmtFile for [$server].[$dbname].$table"
    execAndWait "bcp $table format nul -S $server -d $dbname -c -t$sT -f $frmtFile -T"
    migrator.printTimePassed("Format file created.")
    return frmtFile
}

def getDate = {dateStr ->
    def allowedFormats = [
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        ,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        ,new SimpleDateFormat("yyyy-MM-dd HH:mm")
        ,new SimpleDateFormat("yyyy-MM-dd HH")
        ,new SimpleDateFormat("yyyy-MM-dd")
    ]
    def result = allowedFormats.findResult { format->
        try {
            return format.parse(dateStr)
        }
        catch (ParseException ex) {
            //ignoring since we have bunch of date formats to try
        }
    }
    if (result == null) {
        migrator.terminate "Invalid date str: $dateStr"
    }
    else {
        return result
    }
}

def createIdentity = {idName, idVal ->
    def sqlFileName = "create${idName}.sql"
    def sqlFile = migrator.getFile(sqlFileName, true)
    sqlFile.withWriter {
        it.println """\
                    MERGE [dbo].[idGenerator] AS target
                    USING (SELECT '$idName', $idVal) AS source (identityName, lastValue)
                    ON (target.identityName = source.identityName)
                    WHEN MATCHED THEN 
                        UPDATE SET lastValue = source.lastValue
                    WHEN NOT MATCHED THEN    
                        INSERT (identityName, lastValue)
                        VALUES (source.identityName, source.lastValue);
                """
    }

    execAndWait "sqlcmd -S $server -d $dbname -E -i $sqlFileName"
    migrator.LOGGER.info "Done"
}

//Config
def lastLine

//Execution
migrator.with { 
    source { //loading rainbow table: encrypted -> unencrypted
        inputFile "(?i)top_entity_tab.*"
        outputFile "topEntity.bcp"
        linePattern(/(\w+)$rT.+?$rT(.+?)$rT.*/)
        onMatch { entityId, encrypted -> 
            rainbow[encrypted] = entityId
            "$entityId${sT}1"
        }
        tableName "[dbo].[TopEntity]"
    }

    source { //grabbing code-set A
        inputFile "(?i)code_tab.*"
        outputFile  "Code.bcp"
        onMatch {line -> line} //keeping the same
    }

    source { //grabbing code-set B
        inputFile "(?i)another_code_tab.*"
        outputFile "Code.bcp"
        onMatch {line -> line} //keeping the same
        doLast {
            new File(outputFile).append("?|Code type unknown (migration)|\r\n") //adding new type, just an example
        }
    }

    source { //uploading merged Code.bcp
        inputFile "Code.bcp"
        tableName "[dbo].[Code]"
    }

    source {
        inputFile "(?i)entry_tab.*"
        outputFile "Entry.bcp"
        linePattern(/(^(\w+)$rT([- 0-9:]+)$rT)((?:(\d{4})-(\d{2})-(\d{2}).*?)?$rT)([MD]?)($rT(?:\d{4}-\d{2}-\d{2})?)($rT.*$rT[DWR]$rT.*?$rT)0x.*?$rT(.*)/)
        onMatch { Matcher mtchr -> 
            def master = mtchr.group(2)
            def entryTime = mtchr.group(3)
            def lastUpdated = mtchr.group(11)
            return "${mtchr.group(1)}${toDateString(entryTimeFormat,mtchr.group(5), mtchr.group(6),mtchr.group(7),mtchr.group(8))}${mtchr.group(9)}${mtchr.group(10)}$lastUpdated${sT}${sT}${algyId++}"
        }
    }

    source {
        inputFile "(?i)descr_tab.*"
        outputFile "Desciption.bcp"
        linePattern(/((?:.*?$rT){2})(.*?)?((?:$rT?.*?){3}$rT)(\d{4}-\d{2}-\d{2}$rT.*)/)
        onMatch { Matcher mtchr ->
            def ids = mtchr.group(1)
            def encrypted = ids.substring(ids.indexOf("$sT") + 1, ids.lastIndexOf("$sT"))
            def orig = rainbow[encrypted]
            if (orig != null) {
                ids = ids.replace(encrypted, orig)
                return lastline = "${ids}${mtchr.group(2)}${mtchr.group(3)}${mtchr.group(4)}"
            }
            else {
                if (skippedLinesFileWriter != null) {
                    skippedLinesFileWriter.println "descr_tab\n${mtchr.group(0)}"
                }
                else {
                    terminate "Orphaned descr detected\n ${mtchr.group(0)}"
                }                
            }
            return
        }
        tableName "[dbo].[Description]"
        doLast { 
            LOGGER.info lastline
            createIdentity('DescrId', lastline.substring(0, lastline.indexOf("$sT")))
        }
    }


    upload { table, inFile -> //calling bcp or making a batch file
        formatFile = makeFormatFile(table, inFile)
        LOGGER.info "Uploading data to [$server].[$dbname].$table"
        execAndWait "bcp $table in ${getFileNameFromPath(inFile)} -S $server -d $dbname -b 500000 -m 0 -f $formatFile -T -e skipped_${getFileNameFromPath(inFile).replace('.bcp','')}.txt"
        printTimePassed("Tranfser of ${getFileNameFromPath(inFile)} finished.")
    }

    //doing indexes
    LOGGER.info "Creating indexes"
    execAndWait "sqlcmd -S $server -d $dbname -E -i allIndexes.sql"

    if (options.b) {
        batchFileWriter.println 'echo "Migration successfull!"'
        batchFileWriter.close()
    }

    printTimePassed("Migration successful!")
}
