
# Spark configs
Spark has config settings
that should be modified to suit your specific cluster

```
# read config 
spark.conf.get(<config_settings>)
# write config
spark.conf.set(<config_name>)
```

# Cluster Types
- single nodes - single machine, VM or container
- standalone clusters - with machines as driver + workers
- managed clusters 
   - cluster components handled by 3rd party 
   - eg. YARN / Mesos / Kubernetes  

# Components of a spark cluster
## Driver 
- one driver master per spark cluster
- responsible for assigning task to nodes in the cluster
- monitors state of processes and tasks 
- handles task retries
- consolidates results from any other process in driver
- TIP: Driver node should have double the memory of the worker 
       which will be helpful for task monitoring and data consolidation tasks
- TIP: Fast local storage is also helpful

## Worker
- runs tasks 
- communicates results of tasks back to driver
- ideally already contains the code, data and resources for each task 
   ^ if any of these are unavailable during run time, the worker must pause 
     to obtain the missing resources
- TIPS: more workers is better than large workers
        scaling horizontally is better than scaling vertically
        especially during export/import operations 
- TIPS: Test various configurations to find the right balance of resources for your specific workload. 
- TIPS: In cloud environment, factor in cost of many small machines vs few large machines in cluster 
- TIPS: fast local storage is helpful for caching intermediate files

