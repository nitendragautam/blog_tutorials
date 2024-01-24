## Build Spark Applications

```bash
cd <root_project>
sbt clean package
```

#Copy the Jar file to the Spark Cluster

```bash
scp -p 2222 target/scala-2.11/sparkprojects_2.11-1.0.jar maria_dev@127.0.0.1:/home/maria_dev
```



