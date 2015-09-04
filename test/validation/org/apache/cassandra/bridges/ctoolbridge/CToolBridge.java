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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.nyu.cs.javagit.api.DotGit;
import edu.nyu.cs.javagit.api.Ref;
import edu.nyu.cs.javagit.api.WorkingTree;
import org.apache.cassandra.Node;
import org.apache.cassandra.bridges.ArchiveClusterLogs;
import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.htest.Config;
import org.json.simple.JSONObject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class CToolBridge extends Bridge
{
    private int nodeCount;
    JSONObject cassObj = new JSONObject();
    int bootstrap_attempts = 0;
    int checkCass = 0;
    static final File CASSANDRA_DIR = new File("./");
    private final String DEFAULT_CLUSTER_NAME = "validation";

    private static final Logger logger = LoggerFactory.getLogger(CToolBridge.class);

    public CToolBridge(Config config)
    {
        this(config.nodeCount);
        if (config.cassandrayaml != null)
            updateConf(config.cassandrayaml);
        installAndRunCass();
    }

    public CToolBridge(int nodeCount)
    {
        this.nodeCount = nodeCount;
        if (checkClusterExists())
        {
            if (checkNodeCount(nodeCount))
            {
                execute("ctool reset " + DEFAULT_CLUSTER_NAME);
            }
            else
            {
                execute("ctool destroy " + DEFAULT_CLUSTER_NAME);
                execute("ctool launch %s %d", DEFAULT_CLUSTER_NAME, nodeCount);
            }
        }
        else
        {
            execute("ctool launch %s %d", DEFAULT_CLUSTER_NAME, nodeCount);
        }
    }

    public void destroy()
    {
        stop();
        execute("ctool reset " + DEFAULT_CLUSTER_NAME);
    }

    public void start()
    {
        executeRun("fab -f ~/cstar_perf/tool/cstar_perf/tool/fab_cassandra.py start", "0");
        checkCassRunning();
    }

    public void stop()
    {
        executeRun("fab -f ~/cstar_perf/tool/cstar_perf/tool/fab_cassandra.py stop", "0");
    }

    public void updateConf(Map<String, String> options)
    {
        for (String key : options.keySet())
        {
            if (StringUtils.isNumeric(options.get(key)))
                cassObj.put(key, Integer.parseInt(options.get(key)));
            else
                cassObj.put(key, options.get(key));
        }
    }

    public boolean checkNodeCount(int nodes)
    {
        return nodes == clusterEndpoints().length;
    }

    public void installAndRunCass()
    {
        String cassPath = System.getProperty("user.dir");
        chooseRevision();
        writeToJSON(cassObj, "cassandra.json");
        execute("python " + CASSANDRA_DIR + "/test/validation/org/apache/cassandra/bridges/ctoolbridge/ctool_launch.py " + CASSANDRA_DIR + "/test/validation/org/apache/cassandra/bridges/ctoolbridge/cluster.json");
        add_remote();
        execute("ctool scp -R " + DEFAULT_CLUSTER_NAME + " 0 " + cassPath + " ~/cassandra");
        executeRun("git clone --bare ~/cassandra ~/cassandra.git", "0");
        execute("ctool scp " + DEFAULT_CLUSTER_NAME + " 0 " + CASSANDRA_DIR + "/test/validation/org/apache/cassandra/bridges/ctoolbridge/cassandra.json ~/");
        bootstrapCass();
        stop();
        addBroadcastRPC();
        start();
    }

    public boolean checkClusterExists()
    {
        String result = executeAndRead("ctool list");
        if (result.indexOf(DEFAULT_CLUSTER_NAME) != -1)
            return true;
        else
            return false;
    }

    public String readClusterLogs(String testName)
    {
        String combinedResult = "";
        String existingFolder = CASSANDRA_DIR + "/build/test/logs/validation/ctoolbridge/" + testName;

        if (ArchiveClusterLogs.checkForFolder(existingFolder))
        {
            for (int count = 0; count < nodeCount; count++)
            {
                File searchFile = new File(CASSANDRA_DIR + "/build/test/logs/validation/ctoolbridge/" + testName + "/node" + count + ".log");
                String filePath = searchFile.getAbsolutePath();

                String result = executeAndRead("grep -i error " + filePath);
                combinedResult += result;
            }
        }

        if (ArchiveClusterLogs.countErrors(combinedResult, nodeCount))
            return combinedResult;

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

    public void executeRun(String command, String nodes)
    {

        try
        {
            String[] commandArray = { "ctool", "run", DEFAULT_CLUSTER_NAME, nodes, command };
            Process p = Runtime.getRuntime().exec(commandArray);
            BufferedReader outReaderOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));

            while ((outReaderOutput.readLine()) != null)
            {
            }
            p.waitFor();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public InputStream executeRunAndStream(String command, String nodes)
    {
        try
        {
            String[] commandArray = { "ctool", "run", DEFAULT_CLUSTER_NAME, nodes, command };
            Process p = Runtime.getRuntime().exec(commandArray);
            return p.getInputStream();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void captureLogs(String testName)
    {
        String folderName = testName;
        String existingFolder = CASSANDRA_DIR + "/build/test/logs/validation/ctoolbridge/" + folderName;

        if (ArchiveClusterLogs.checkForFolder(existingFolder))
        {
            ArchiveClusterLogs.zipExistingDirectory(existingFolder);
        }

        for (int count = 0; count < nodeCount; count++)
        {
            String sourceFile = "/home/automaton/fab/cassandra/logs/system.log";
            String destFile = ArchiveClusterLogs.getFullPath(new File(CASSANDRA_DIR + "/build/test/logs/validation/ctoolbridge/" + folderName + "/node" + count + ".log"), existingFolder);
            execute("ctool scp -r " + DEFAULT_CLUSTER_NAME + " " + count + " " + destFile + " " + sourceFile);
        }
    }

    public void nodeTool(Node node, String command, String arguments)
    {
        String hostname = clusterEndpoints()[Integer.parseInt(node.getName())];
        String fullCommand;
        if (Objects.equals(arguments, ""))
        {
            fullCommand = "/home/automaton/fab/cassandra/bin/nodetool -h " + hostname + " " + command;
        }
        else
        {
            fullCommand = "/home/automaton/fab/cassandra/bin/nodetool -h " + hostname + " " + command + " " + arguments;
        }

        InputStream output = executeRunAndStream(fullCommand, node.getName());
        printStream(output);
    }

    public String[] clusterEndpoints()
    {
        String result = executeAndRead("ctool info " + DEFAULT_CLUSTER_NAME + " --hosts");
        result = result.substring(0, result.length() - 1);
        String[] endpoints = result.split(" ");
        return endpoints;
    }

    public void ssTableSplit(Node node, String options, String keyspace_path)
    {
        String fullCommand;
        if (Objects.equals(options, ""))
        {
            fullCommand = "/home/automaton/fab/cassandra/tools/bin/sstablesplit /mnt/data1/cassandra/data/" + keyspace_path;
        }
        else
        {
            fullCommand = "/home/automaton/fab/cassandra/tools/bin/sstablesplit " + options + " /mnt/data1/cassandra/data/" + keyspace_path;
        }

        InputStream output = executeRunAndStream(fullCommand, node.getName());
        printStream(output);
    }

    public void ssTableMetaData(Node node, String keyspace_path)
    {
        String fullCommand = "/home/automaton/fab/cassandra/tools/bin/sstablemetadata /mnt/data1/cassandra/data/" + keyspace_path;
        InputStream output = executeRunAndStream(fullCommand, node.getName());
        printStream(output);
    }

    public String stress(String options)
    {
        throw new NotImplementedException();
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

    public String local_repo()
    {
        String result = executeAndRead("ctool info " + DEFAULT_CLUSTER_NAME + " --private-ips");
        result = result.substring(0, result.length() - 1);
        String[] endpoints = result.split(" ");
        return "automaton@ip-" + endpoints[0].replaceAll("\\.", "-") + ":~/cassandra.git";
    }

    public void chooseRevision()
    {
        try
        {
            String cassPath = System.getProperty("user.dir");
            File repoDir = new File(cassPath);
            DotGit dotGit = DotGit.getInstance(repoDir);
            WorkingTree wt = dotGit.getWorkingTree();
            Ref currentBranch = wt.getCurrentBranch();
            cassObj.put("revision", "local/" + currentBranch.toString());
        }
        catch (Exception e)
        {
            throw new RuntimeException();
        }
    }

    public void writeToJSON(JSONObject object, String filename)
    {
        try
        {
            File file = new File(CASSANDRA_DIR + "/test/validation/org/apache/cassandra/bridges/ctoolbridge/" + filename);
            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(object.toJSONString());
            fileWriter.flush();
            fileWriter.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException();
        }
    }

    public void add_remote()
    {
        JSONObject git_remote = new JSONObject();
        git_remote.put("local", local_repo());
        writeToJSON(git_remote, "git_remotes.json");
        execute("ctool scp " + DEFAULT_CLUSTER_NAME + " 0 " + CASSANDRA_DIR + "/test/validation/org/apache/cassandra/bridges/ctoolbridge/git_remotes.json ~/.cstar_perf");
    }

    public void bootstrapCass()
    {
        bootstrap_attempts += 1;
        InputStream output = executeRunAndStream("cstar_perf_bootstrap ~/cassandra.json", "0");
        Reader outReaderOutput = new BufferedReader(new InputStreamReader(output));
        if (streamContainsString(outReaderOutput, "All nodes available!"))
        {
            return;
        }
        else
        {
            if (bootstrap_attempts > 5)
                throw new RuntimeException("Unable to bootstrap Cassandra!!!");
            bootstrapCass();
        }
    }

    public boolean streamContainsString(Reader reader, String searchString)
    {
        Scanner streamScanner = new Scanner(reader);
        if (streamScanner.findWithinHorizon(searchString, 0) != null)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    public void addBroadcastRPC()
    {
        String[] endpoints = clusterEndpoints();

        for (int count = 0; count < nodeCount; count++)
        {
            String node = Integer.toString(count);

            if(endpoints[count].indexOf("ec2") != -1)
            {
                String[] split = endpoints[count].split("\\.");
                String ec2ip = split[0].substring(4);
                String public_ip = ec2ip.replaceAll("\\-", ".");
                executeRun("echo \"broadcast_rpc_address: " + public_ip + "\" | tee -a ~/fab/cassandra/conf/cassandra.yaml", node);
            }
            else
            {
                executeRun("echo \"broadcast_rpc_address: " + endpoints[count] + "\" | tee -a ~/fab/cassandra/conf/cassandra.yaml", node);
            }
        }
    }

    public void checkCassRunning()
    {
        checkCass += 1;

        if(checkCass <= 6)
        {
            try
            {
                Thread.sleep(10000);
                Cluster cluster = Cluster.builder().addContactPoints(clusterEndpoints()[0]).build();
                Session session = cluster.connect();
                session.close();
                cluster.close();
            }
            catch (Exception e)
            {
                checkCassRunning();
            }
        }
        else
        {
            throw new RuntimeException("Unable to start Cassandra!!!");
        }
    }
}
