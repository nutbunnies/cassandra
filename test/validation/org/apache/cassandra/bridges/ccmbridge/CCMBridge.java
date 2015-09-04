/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.cassandra.bridges.ccmbridge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Node;
import org.apache.cassandra.bridges.ArchiveClusterLogs;
import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.htest.Config;
import org.apache.cassandra.tools.SSTableMetadataViewer;

public class CCMBridge extends Bridge
{

    private int nodeCount;
    static final File CASSANDRA_DIR = new File("./");
    private final File ccmDir;
    private final String DEFAULT_CLUSTER_NAME = "validation";

    private static final Logger logger = LoggerFactory.getLogger(CCMBridge.class);

    public CCMBridge(Config config)
    {
        this(config.nodeCount);
        if (config.cassandrayaml != null)
            updateConf(config.cassandrayaml);
        start();
    }

    public CCMBridge(int nodeCount)
    {
        this.nodeCount = nodeCount;
        this.ccmDir = Files.createTempDir();
        removeOldCluster();
        ArchiveClusterLogs.savetempDirectoryPath(CASSANDRA_DIR, ccmDir);
        execute("ccm create %s -n %d --install-dir %s", DEFAULT_CLUSTER_NAME, nodeCount, CASSANDRA_DIR);
    }

    public void destroy()
    {
        stop();
        execute("ccm remove");
    }

    public void start()
    {
        execute("ccm start");
    }

    public void stop()
    {
        execute("ccm stop");
    }

    public void forceStop()
    {
        execute("ccm stop --not-gently");
    }

    public String readClusterLogs(String testName)
    {
        return executeAndRead("ccm checklogerror");
    }

    public void updateConf(Map<String, String> options)
    {
        for (String key : options.keySet())
            execute("ccm updateconf %s:%s", key, options.get(key));
    }

    private void execute(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
            int retValue = p.waitFor();

            if (retValue != 0)
            {
                BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                String line = outReader.readLine();
                while (line != null)
                {
                    logger.info("out> " + line);
                    line = outReader.readLine();
                }
                line = errReader.readLine();
                while (line != null)
                {
                    logger.error("err> " + line);
                    line = errReader.readLine();
                }
                throw new RuntimeException();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void executeAndPrint(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);

            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = outReaderOutput.readLine();
            while (line != null)
            {
                System.out.println(line);
                line = outReaderOutput.readLine();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String executeAndRead(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);

            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = outReaderOutput.readLine();
            String output = "";

            while (line != null)
            {
                output += line + "\n";
                line = outReaderOutput.readLine();
            }
            return output;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private InputStream executeAndStream(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
            return p.getInputStream();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private InputStream executeAndReadWithToolOptions(String command, String options)
    {
        try
        {
            String fullCommand = command + " --config-dir=" + ccmDir + String.format(" -- %s", options);
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
            return p.getInputStream();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String executeAndReturn(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args) + " --config-dir=" + ccmDir;
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);

            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = outReaderOutput.readLine();
            return line;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void captureLogs(String testName)
    {
        String clusterLogsPath = ccmDir + "/" + DEFAULT_CLUSTER_NAME;
        String folderName = testName;
        String existingFolder = CASSANDRA_DIR + "/build/test/logs/validation/ccmbridge/" + folderName;

        if(ArchiveClusterLogs.checkForFolder(existingFolder))
        {
            ArchiveClusterLogs.zipExistingDirectory(existingFolder);
        }

        for(int count = 1; count <= nodeCount; count++)
        {
            File sourceFile = new File(clusterLogsPath + "/node" + count + "/logs" + "/system.log");

            File destFile = new File(CASSANDRA_DIR + "/build/test/logs/validation/ccmbridge/" + folderName + "/node" + count + ".log");

            try
            {
                FileUtils.copyFile(sourceFile, destFile);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void nodeTool(Node node, String command, String arguments)
    {
        String fullCommand;
        if(Objects.equals(arguments, ""))
        {
            fullCommand = "ccm node" + node.getName() + " nodetool " + command;
        }
        else
        {
            fullCommand = "ccm node" + node.getName() + " nodetool " + command + " " + arguments;
        }

        InputStream output = executeAndStream(fullCommand);
        printStream(output);
    }

    public void removeOldCluster()
    {
        File filePath = new File(CASSANDRA_DIR + "/build/test/logs/validation/tempDir.txt");

        if (filePath.exists())
        {
            try
            {
                BufferedReader br = new BufferedReader(new FileReader(filePath));
                String line = br.readLine();
                String tmpDir = null;

                while(line != null){

                    tmpDir = line;
                    line = br.readLine();
                }

                File tempDir = new File(tmpDir);
                if (tempDir.exists())
                {
                    if(tempDir.list().length > 0)
                    {
                        removeLiveCluster(tmpDir);
                    }
                }
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void removeLiveCluster(String temp_dir)
    {
        String fullCommand = "ccm remove" + " --config-dir=" + temp_dir;
        try
        {
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null, CASSANDRA_DIR);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public String[] clusterEndpoints()
    {
        String result = executeAndRead("ccm liveset");
        result = result.substring(0, result.length() - 1);
        String[] endpoints = result.split(",");
        return endpoints;
    }

    public void ssTableSplit(Node node, String options, String keyspace)
    {
        String fullCommand;
        if(Objects.equals(options, ""))
        {
            fullCommand = "ccm node" + node.getName() + " sstablesplit";
        }
        else
        {
            fullCommand = "ccm node" + node.getName() + " sstablesplit " + options;
        }

        InputStream output = executeAndStream(fullCommand);
        printStream(output);
    }

    public void ssTableMetaData(Node node, String keyspace)
    {
        String fullCommand;
        if(Objects.equals(keyspace, ""))
        {
            fullCommand = "ccm node" + node.getName() + " getsstables";
        }
        else
        {
            fullCommand = "ccm node" + node.getName() + " getsstables " + keyspace;
        }

        String sstableFiles = executeAndReturn(fullCommand);

        if(sstableFiles.length() > 2)
        {
            String[] files = ArchiveClusterLogs.metaDataFilesFormatter(sstableFiles);
            try
            {
                SSTableMetadataViewer.main(files);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            System.out.println("No SSTables Found.");
        }
    }

    public String stress(String options)
    {
        try
        {
            String command = "ccm stress ";
            InputStream result = executeAndReadWithToolOptions(command, options);
            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(result));
            String line = outReaderOutput.readLine();
            String output = "";

            while (line != null)
            {
                output += line + "\n";
                line = outReaderOutput.readLine();
            }
            return output;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

    }

    public void printStream(InputStream output)
    {
        try
        {
            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(output));
            String line = outReaderOutput.readLine();
            while (line != null)
            {
                System.out.println(line);
                line = outReaderOutput.readLine();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}

