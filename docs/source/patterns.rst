Usage Patterns
==============

* List all clusters and shut them down ::

  for cluster in gateway.list_clusters():
      gateway.stop_cluster(cluster.name)
