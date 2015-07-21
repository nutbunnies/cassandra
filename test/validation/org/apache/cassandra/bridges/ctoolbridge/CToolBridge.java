/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.bridges.ctoolbridge;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridges.ArchiveClusterLogs;
import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.htest.Config;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class CToolBridge extends Bridge
{
    private int nodeCount;
    static final File CASSANDRA_DIR = new File("./");
    private final String DEFAULT_CLUSTER_NAME = "CVH";

    private static final Logger logger = LoggerFactory.getLogger(CToolBridge.class);

    public CToolBridge(Config config)
    {
        this(config.nodeCount);
        if (config.cassandrayaml != null)
            updateConf(config.cassandrayaml);
        start();
    }

    public CToolBridge(int nodeCount)
    {
        this.nodeCount = nodeCount;
        if(checkClusterExists())
        {
            if(checkNodeCount(nodeCount))
            {
                execute("ctool reset " + DEFAULT_CLUSTER_NAME);
                installCass();
            }
            else
            {
                execute("ctool destroy " + DEFAULT_CLUSTER_NAME);
                execute("ctool launch %s %d", DEFAULT_CLUSTER_NAME, nodeCount);
                installCass();
            }
        }
        else
        {
            execute("ctool launch %s %d", DEFAULT_CLUSTER_NAME, nodeCount);
            installCass();
        }
    }

    public void destroy()
    {
        stop();
        execute("ctool reset " + DEFAULT_CLUSTER_NAME);
    }

    public void start()
    {
        String command = "/home/automaton/cassandra/bin/cassandra -p ~/PID";
        String nodes = "all";
        executeRun(command, nodes);
        String process_ids = CASSANDRA_DIR + "/build/test/logs/validation/PIDs";
        if(!ArchiveClusterLogs.checkForFolder(process_ids))
        {
            File pids = new File(CASSANDRA_DIR + "/build/test/logs/validation/PIDs");
            pids.mkdirs();
        }
        for(int count = 0; count < nodeCount; count++)
        {
            int nodeCount = count + 1;
            execute("ctool scp -r " + DEFAULT_CLUSTER_NAME + " " + count + " " + process_ids + "/node" + nodeCount + "_PID.txt " + "~/PID");
        }
    }

    public void stop()
    {
        String process_ids = CASSANDRA_DIR + "/build/test/logs/validation/PIDs";

        for(int count = 0; count < nodeCount; count++)
        {
            int nodeCount = count + 1;
            String pid = executeAndRead("cat " + process_ids + "/node" + nodeCount + "_PID.txt");
            executeRun("kill " + pid.replaceAll("\n", ""), Integer.toString(count));
        }
    }

    public void updateConf(Map<String, String> options)
    {
        for (String key : options.keySet())
            execute("ctool change_config %s all --k %s --value %s", DEFAULT_CLUSTER_NAME, key, options.get(key));
    }

    public boolean checkNodeCount(int nodes)
    {
        return nodes == clusterEndpoints().length;
    }

    public void installCass()
    {
        String cassPath =  System.getProperty("user.dir");
        execute("ctool scp " + DEFAULT_CLUSTER_NAME + " all " + cassPath + " ~/cassandra");
        //execute("python " + CASSANDRA_DIR + "/test/validation/org/apeache/cassandra/bridges/ctoolbridge/ctool_launch.py cluster.json");
    }

    public boolean checkClusterExists()
    {
        String result = executeAndRead("ctool list");
        if(result.indexOf(DEFAULT_CLUSTER_NAME) != -1)
            return true;
        else
            return false;
    }

    public String readClusterLogs(String testName)
    {
        String combinedResult = "";
        String existingFolder = CASSANDRA_DIR + "/build/test/logs/validation/" + testName;

        if(ArchiveClusterLogs.checkForFolder(existingFolder))
        {
            for(int count = 1; count <= nodeCount; count++)
            {
                File searchFile = new File(CASSANDRA_DIR + "/build/test/logs/validation/" + testName + "/node" + count + ".log");
                String filePath = searchFile.getAbsolutePath();

                String result = executeAndRead("grep -i error " + filePath);
                combinedResult += result;
            }
        }

        if (ArchiveClusterLogs.countErrors(combinedResult))
            return combinedResult;
        else
            return "";
    }

    private void execute(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args);
            logger.debug("Executing: " + fullCommand);
            Process p = runtime.exec(fullCommand, null);
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
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void executeAndPrint(String command, Object... args)
    {
        try
        {
            String fullCommand = String.format(command, args);
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
            String fullCommand = String.format(command, args);
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

    public void executeRun(String command, String nodes){

        try
        {
            String[] commandArray = {"ctool", "run", DEFAULT_CLUSTER_NAME, nodes, command};
            Process p = Runtime.getRuntime().exec(commandArray);

            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = outReaderOutput.readLine();

            while (line != null)
            {
                System.out.println(line);
                line = outReaderOutput.readLine();
            }

            p.destroy();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void captureLogs(String testName)
    {
        String folderName = testName;
        String existingFolder = CASSANDRA_DIR + "/build/test/logs/validation/" + folderName;

        if(ArchiveClusterLogs.checkForFolder(existingFolder))
        {
            ArchiveClusterLogs.zipExistingDirectory(existingFolder);
        }

        for(int count = 0; count < nodeCount; count++)
        {
            int logNameCount = count + 1;
            String sourceFile = "/home/automaton/cassandra/logs/system.log";
            String destFile = ArchiveClusterLogs.getFullPath(new File(CASSANDRA_DIR + "/build/test/logs/validation/" + folderName + "/node" + logNameCount + ".log"), existingFolder);

            execute("ctool scp -r " + DEFAULT_CLUSTER_NAME + " " + count + " " + destFile + " " + sourceFile);
        }
    }

    public void nodeTool(String node, String command, String arguments)
    {
        String fullCommand;
        if (arguments == "")
        {
            fullCommand = "/home/automaton/cassandra/bin/nodetool " + command;
            executeRun(fullCommand, node);
        }
        else{
            fullCommand = "/home/automaton/cassandra/bin/nodetool " + command + " " + arguments;
            executeRun(fullCommand, node);
        }
    }

    public String[] clusterEndpoints()
    {
        String result = executeAndRead("ctool info " + DEFAULT_CLUSTER_NAME + " --hosts");
        result = result.substring(0, result.length() - 1);
        String[] endpoints = result.split(" ");
        return endpoints;
    }

    public void ssTableSplit(String node, String options, String keyspace_path)
    {
        String fullCommand;
        if (options == "")
        {
            fullCommand = "/home/automaton/cassandra/tools/bin/sstablesplit /home/automaton/cassandra/data/data/" + keyspace_path;
            executeRun(fullCommand, node);
        }
        else
        {
            fullCommand = "/home/automaton/cassandra/tools/bin/sstablesplit " + options + " /home/automaton/cassandra/data/data/" + keyspace_path;
            executeRun(fullCommand, node);
        }
        executeAndPrint(fullCommand);
    }

    public void ssTableMetaData(String node, String keyspace_path)
    {
        String fullCommand = "/home/automaton/cassandra/tools/bin/sstablemetadata /home/automaton/cassandra/data/data/" + keyspace_path;
        executeRun(fullCommand, node);
    }

    public String stress(String options)
    {
        throw new NotImplementedException();
    }
}
