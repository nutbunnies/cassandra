/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridges.Bridge;
import org.apache.cassandra.bridges.ccmbridge.CCMBridge;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.htest.Config;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.modules.Module;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

@RunWith(Parameterized.class)
public class HarnessTest
{
    private static final Logger logger = LoggerFactory.getLogger(HarnessTest.class);
    public static final String MODULE_PACKAGE = "org.apache.cassandra.modules.";
    private String yaml;
    private Config config;
    private Bridge cluster;
    ArrayList<Future> module_exceptions = new ArrayList<>();
    private Map<String, List<String>> failures = new HashMap<>();
    private DebuggableThreadPoolExecutor executor = new DebuggableThreadPoolExecutor("Harness", Thread.NORM_PRIORITY);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> discoverTests()
    {
        File folder = new File("../cassandra/test/validation/org/apache/cassandra/htest");
        return Arrays.stream(folder.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(".yaml")))
                                   .map(file -> new Object[]{FileUtils.getCanonicalPath(file)})
                                   .collect(Collectors.toList());
    }

    public HarnessTest(String yamlParameter)
    {
        yaml = yamlParameter;
    }

    @Test
    public void harness()
    {
        config = loadConfig(getConfigURL(yaml));
        cluster = new CCMBridge(config);
        HarnessContext context = new HarnessContext(this, cluster);
        for (String[] moduleGroup : config.modules)
        {
            List<Module> modules = Arrays.stream(moduleGroup)
                                         .map(moduleName -> reflectModuleByName(moduleName, config, context))
                                         .collect(Collectors.toList());
            runModuleGroup(modules);
        }
    }

    @After
    public void tearDown()
    {
        failures.values().forEach(failureList -> failureList.forEach(logger::error));
        cluster.stop();
        cluster.captureLogs(getTestName(yaml));
        String result = cluster.readClusterLogs(getTestName(yaml));
        cluster.destroy();
        Assert.assertTrue(result, Objects.equals(result, ""));
        parseFailures(failures);
    }

    public void cleanConfig(Config config)
    {
        if (config.ignoredErrors == null)
        {
            config.ignoredErrors = new ArrayList<>();
        }
        if (config.requiredErrors == null)
        {
            config.requiredErrors = new ArrayList<>();
        }
    }

    public void parseFailures(Map<String, List<String>> failures)
    {
        for(String moduleName : failures.keySet())
        {
            for(String failure : failures.get(moduleName))
            {
                for(String error : config.ignoredErrors)
                {
                    if(failure.contains(error))
                    {
                        failures.get(moduleName).remove(failure);
                    }
                }

                for(String error : config.requiredErrors)
                {
                    if(failure.contains(error))
                    {
                        config.requiredErrors.remove(error);
                    }
                }
            }
            if (failures.get(moduleName).isEmpty())
            {
                failures.remove(moduleName);
            }
        }

        if(!failures.isEmpty())
        {
            Assert.fail();
        }

        if(!config.requiredErrors.isEmpty())
        {
            Assert.fail();
        }
    }

    public void signalFailure(String moduleName, String message)
    {
        Future exception = newTask(new FailureTask(moduleName, message));
        module_exceptions.add(exception);
    }

    private Future newTask(Runnable task)
    {
        return executor.submit(task);
    }

    public void runModuleGroup(final List<Module> modules)
    {
        List<Future> futures = modules.stream().map(Module::validate).collect(Collectors.toList());

        try
        {
            for (Future future : futures)
            {
                future.get();
            }

            for (Future future : module_exceptions)
            {
                future.get();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Module reflectModuleByName(String moduleName, Config config, HarnessContext context)
    {
        try
        {
            return (Module) Class.forName(MODULE_PACKAGE + moduleName)
                                 .getDeclaredConstructor(new Class[]{Config.class, HarnessContext.class}).newInstance(config, context);
        }
        // ClassNotFoundException
        // NoSuchMethodException
        // InvocationTargetException
        // InstantiationException
        // IllegalAccessException
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Config loadConfig(URL url)
    {
        try
        {
            byte[] configBytes;
            try (InputStream is = url.openStream())
            {
                configBytes = ByteStreams.toByteArray(is);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
            org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(Config.class);
            Yaml yaml = new Yaml(constructor);
            Config result = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
            cleanConfig(result);
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml: " + url, e);
        }
    }

    static URL getConfigURL(String yamlPath)
    {
        URL url;
        try
        {
            url = new URL("file:" + File.separator + File.separator + yamlPath);
            url.openStream().close(); // catches well-formed but bogus URLs
            return url;
        }
        catch (Exception e)
        {
            throw new AssertionError("Yaml path was invalid", e);
        }
    }

    static String getTestName(String yamlPath)
    {
        Path p = Paths.get(yamlPath);
        String file = p.getFileName().toString();
        String testName = file.substring(0, file.lastIndexOf('.'));
        return testName;
    }

    class FailureTask implements Runnable
    {
        private String moduleName;
        private String message;

        public FailureTask(String moduleName, String message)
        {
            this.moduleName = moduleName;
            this.message = message;
        }

        public void run()
        {
            this.message = this.moduleName + ": " + this.message;
            if (failures.containsKey(moduleName))
            {
                failures.get(moduleName).add(message);
            }
            else
            {
                ArrayList<String> failure = new ArrayList<>();
                failure.add(message);
                failures.put(moduleName, failure);
            }
        }
    }
}
