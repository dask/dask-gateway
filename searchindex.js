Search.setIndex({docnames:["api-client","api-server","authentication","changelog","cluster-options","configuration-user","develop","index","install-hadoop","install-jobqueue","install-kube","install-local","install-user","resource-limits","security","usage"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":5,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":3,"sphinx.domains.rst":2,"sphinx.domains.std":2,sphinx:56},filenames:["api-client.rst","api-server.rst","authentication.rst","changelog.md","cluster-options.rst","configuration-user.rst","develop.rst","index.rst","install-hadoop.rst","install-jobqueue.rst","install-kube.rst","install-local.rst","install-user.rst","resource-limits.rst","security.rst","usage.rst"],objects:{"dask_gateway.Gateway":[[0,1,1,"","adapt_cluster"],[0,1,1,"","close"],[0,1,1,"","cluster_options"],[0,1,1,"","connect"],[0,1,1,"","get_cluster"],[0,1,1,"","get_versions"],[0,1,1,"","list_clusters"],[0,1,1,"","new_cluster"],[0,1,1,"","scale_cluster"],[0,1,1,"","stop_cluster"],[0,1,1,"","submit"]],"dask_gateway.GatewayCluster":[[0,1,1,"","adapt"],[0,1,1,"","close"],[0,1,1,"","from_name"],[0,1,1,"","get_client"],[0,1,1,"","scale"],[0,1,1,"","shutdown"]],"dask_gateway.auth":[[0,0,1,"","BasicAuth"],[0,0,1,"","GatewayAuth"],[0,0,1,"","JupyterHubAuth"],[0,0,1,"","KerberosAuth"]],"dask_gateway.options":[[0,0,1,"","Options"]],"dask_gateway_server.app":[[1,0,1,"","DaskGateway"]],"dask_gateway_server.app.DaskGateway":[[1,2,1,"","address"],[1,2,1,"","authenticator_class"],[1,2,1,"","backend_class"],[1,2,1,"","config_file"],[1,2,1,"","log_datefmt"],[1,2,1,"","log_format"],[1,2,1,"","log_level"],[1,2,1,"","show_config"],[1,2,1,"","show_config_json"]],"dask_gateway_server.auth":[[1,0,1,"","JupyterHubAuthenticator"],[1,0,1,"","KerberosAuthenticator"],[1,0,1,"","SimpleAuthenticator"]],"dask_gateway_server.auth.JupyterHubAuthenticator":[[1,2,1,"","cache_max_age"],[1,2,1,"","cookie_name"],[1,2,1,"","jupyterhub_api_token"],[1,2,1,"","jupyterhub_api_url"],[1,2,1,"","tls_ca"],[1,2,1,"","tls_cert"],[1,2,1,"","tls_key"]],"dask_gateway_server.auth.KerberosAuthenticator":[[1,2,1,"","cache_max_age"],[1,2,1,"","cookie_name"],[1,2,1,"","keytab"],[1,2,1,"","service_name"]],"dask_gateway_server.auth.SimpleAuthenticator":[[1,2,1,"","cache_max_age"],[1,2,1,"","cookie_name"],[1,2,1,"","password"]],"dask_gateway_server.backends.base":[[1,0,1,"","Backend"],[1,0,1,"","ClusterConfig"]],"dask_gateway_server.backends.base.Backend":[[1,2,1,"","api_url"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_options"]],"dask_gateway_server.backends.base.ClusterConfig":[[1,2,1,"","adaptive_period"],[1,2,1,"","cluster_max_cores"],[1,2,1,"","cluster_max_memory"],[1,2,1,"","cluster_max_workers"],[1,2,1,"","environment"],[1,2,1,"","idle_timeout"],[1,2,1,"","scheduler_cmd"],[1,2,1,"","scheduler_cores"],[1,2,1,"","scheduler_memory"],[1,2,1,"","worker_cmd"],[1,2,1,"","worker_cores"],[1,2,1,"","worker_memory"],[1,2,1,"","worker_threads"]],"dask_gateway_server.backends.jobqueue.pbs":[[1,0,1,"","PBSBackend"],[1,0,1,"","PBSClusterConfig"]],"dask_gateway_server.backends.jobqueue.pbs.PBSBackend":[[1,2,1,"","api_url"],[1,2,1,"","backoff_base_delay"],[1,2,1,"","backoff_max_delay"],[1,2,1,"","cancel_command"],[1,2,1,"","check_timeouts_period"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_heartbeat_period"],[1,2,1,"","cluster_heartbeat_timeout"],[1,2,1,"","cluster_options"],[1,2,1,"","cluster_start_timeout"],[1,2,1,"","cluster_status_period"],[1,2,1,"","dask_gateway_jobqueue_launcher"],[1,2,1,"","db_cleanup_period"],[1,2,1,"","db_cluster_max_age"],[1,2,1,"","db_debug"],[1,2,1,"","db_encrypt_keys"],[1,2,1,"","db_url"],[1,2,1,"","gateway_hostname"],[1,2,1,"","parallelism"],[1,2,1,"","status_command"],[1,2,1,"","stop_clusters_on_shutdown"],[1,2,1,"","submit_command"],[1,2,1,"","worker_start_failure_limit"],[1,2,1,"","worker_start_timeout"],[1,2,1,"","worker_status_period"]],"dask_gateway_server.backends.jobqueue.pbs.PBSClusterConfig":[[1,2,1,"","account"],[1,2,1,"","adaptive_period"],[1,2,1,"","cluster_max_cores"],[1,2,1,"","cluster_max_memory"],[1,2,1,"","cluster_max_workers"],[1,2,1,"","environment"],[1,2,1,"","idle_timeout"],[1,2,1,"","project"],[1,2,1,"","queue"],[1,2,1,"","scheduler_cmd"],[1,2,1,"","scheduler_cores"],[1,2,1,"","scheduler_memory"],[1,2,1,"","scheduler_resource_list"],[1,2,1,"","scheduler_setup"],[1,2,1,"","staging_directory"],[1,2,1,"","use_stagein"],[1,2,1,"","worker_cmd"],[1,2,1,"","worker_cores"],[1,2,1,"","worker_memory"],[1,2,1,"","worker_resource_list"],[1,2,1,"","worker_setup"],[1,2,1,"","worker_threads"]],"dask_gateway_server.backends.jobqueue.slurm":[[1,0,1,"","SlurmBackend"],[1,0,1,"","SlurmClusterConfig"]],"dask_gateway_server.backends.jobqueue.slurm.SlurmBackend":[[1,2,1,"","api_url"],[1,2,1,"","backoff_base_delay"],[1,2,1,"","backoff_max_delay"],[1,2,1,"","cancel_command"],[1,2,1,"","check_timeouts_period"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_heartbeat_period"],[1,2,1,"","cluster_heartbeat_timeout"],[1,2,1,"","cluster_options"],[1,2,1,"","cluster_start_timeout"],[1,2,1,"","cluster_status_period"],[1,2,1,"","dask_gateway_jobqueue_launcher"],[1,2,1,"","db_cleanup_period"],[1,2,1,"","db_cluster_max_age"],[1,2,1,"","db_debug"],[1,2,1,"","db_encrypt_keys"],[1,2,1,"","db_url"],[1,2,1,"","parallelism"],[1,2,1,"","status_command"],[1,2,1,"","stop_clusters_on_shutdown"],[1,2,1,"","submit_command"],[1,2,1,"","worker_start_failure_limit"],[1,2,1,"","worker_start_timeout"],[1,2,1,"","worker_status_period"]],"dask_gateway_server.backends.jobqueue.slurm.SlurmClusterConfig":[[1,2,1,"","account"],[1,2,1,"","adaptive_period"],[1,2,1,"","cluster_max_cores"],[1,2,1,"","cluster_max_memory"],[1,2,1,"","cluster_max_workers"],[1,2,1,"","environment"],[1,2,1,"","idle_timeout"],[1,2,1,"","partition"],[1,2,1,"","qos"],[1,2,1,"","scheduler_cmd"],[1,2,1,"","scheduler_cores"],[1,2,1,"","scheduler_memory"],[1,2,1,"","scheduler_setup"],[1,2,1,"","staging_directory"],[1,2,1,"","worker_cmd"],[1,2,1,"","worker_cores"],[1,2,1,"","worker_memory"],[1,2,1,"","worker_setup"],[1,2,1,"","worker_threads"]],"dask_gateway_server.backends.kubernetes":[[1,0,1,"","KubeBackend"],[1,0,1,"","KubeClusterConfig"]],"dask_gateway_server.backends.kubernetes.KubeBackend":[[1,2,1,"","api_url"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_options"],[1,2,1,"","common_annotations"],[1,2,1,"","common_labels"],[1,2,1,"","crd_version"],[1,2,1,"","gateway_instance"],[1,2,1,"","label_selector"]],"dask_gateway_server.backends.kubernetes.KubeClusterConfig":[[1,2,1,"","adaptive_period"],[1,2,1,"","cluster_max_cores"],[1,2,1,"","cluster_max_memory"],[1,2,1,"","cluster_max_workers"],[1,2,1,"","environment"],[1,2,1,"","idle_timeout"],[1,2,1,"","image"],[1,2,1,"","image_pull_policy"],[1,2,1,"","image_pull_secrets"],[1,2,1,"","namespace"],[1,2,1,"","scheduler_cmd"],[1,2,1,"","scheduler_cores"],[1,2,1,"","scheduler_cores_limit"],[1,2,1,"","scheduler_extra_container_config"],[1,2,1,"","scheduler_extra_pod_annotations"],[1,2,1,"","scheduler_extra_pod_config"],[1,2,1,"","scheduler_extra_pod_labels"],[1,2,1,"","scheduler_memory"],[1,2,1,"","scheduler_memory_limit"],[1,2,1,"","worker_cmd"],[1,2,1,"","worker_cores"],[1,2,1,"","worker_cores_limit"],[1,2,1,"","worker_extra_container_config"],[1,2,1,"","worker_extra_pod_annotations"],[1,2,1,"","worker_extra_pod_config"],[1,2,1,"","worker_extra_pod_labels"],[1,2,1,"","worker_memory"],[1,2,1,"","worker_memory_limit"],[1,2,1,"","worker_threads"]],"dask_gateway_server.backends.kubernetes.controller":[[1,0,1,"","KubeController"]],"dask_gateway_server.backends.kubernetes.controller.KubeController":[[1,2,1,"","address"],[1,2,1,"","api_url"],[1,2,1,"","backoff_base_delay"],[1,2,1,"","backoff_max_delay"],[1,2,1,"","common_annotations"],[1,2,1,"","common_labels"],[1,2,1,"","completed_cluster_cleanup_period"],[1,2,1,"","completed_cluster_max_age"],[1,2,1,"","config_file"],[1,2,1,"","crd_version"],[1,2,1,"","gateway_instance"],[1,2,1,"","k8s_api_rate_limit"],[1,2,1,"","k8s_api_rate_limit_burst"],[1,2,1,"","label_selector"],[1,2,1,"","log_datefmt"],[1,2,1,"","log_format"],[1,2,1,"","log_level"],[1,2,1,"","parallelism"],[1,2,1,"","proxy_prefix"],[1,2,1,"","proxy_tcp_entrypoint"],[1,2,1,"","proxy_web_entrypoint"],[1,2,1,"","proxy_web_middlewares"],[1,2,1,"","show_config"],[1,2,1,"","show_config_json"]],"dask_gateway_server.backends.local":[[1,0,1,"","LocalBackend"],[1,0,1,"","LocalClusterConfig"],[1,0,1,"","UnsafeLocalBackend"]],"dask_gateway_server.backends.local.LocalBackend":[[1,2,1,"","api_url"],[1,2,1,"","backoff_base_delay"],[1,2,1,"","backoff_max_delay"],[1,2,1,"","check_timeouts_period"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_heartbeat_period"],[1,2,1,"","cluster_heartbeat_timeout"],[1,2,1,"","cluster_options"],[1,2,1,"","cluster_start_timeout"],[1,2,1,"","cluster_status_period"],[1,2,1,"","clusters_directory"],[1,2,1,"","db_cleanup_period"],[1,2,1,"","db_cluster_max_age"],[1,2,1,"","db_debug"],[1,2,1,"","db_encrypt_keys"],[1,2,1,"","db_url"],[1,2,1,"","inherited_environment"],[1,2,1,"","parallelism"],[1,2,1,"","sigint_timeout"],[1,2,1,"","sigkill_timeout"],[1,2,1,"","sigterm_timeout"],[1,2,1,"","stop_clusters_on_shutdown"],[1,2,1,"","worker_start_failure_limit"],[1,2,1,"","worker_start_timeout"],[1,2,1,"","worker_status_period"]],"dask_gateway_server.backends.local.LocalClusterConfig":[[1,2,1,"","adaptive_period"],[1,2,1,"","cluster_max_cores"],[1,2,1,"","cluster_max_memory"],[1,2,1,"","cluster_max_workers"],[1,2,1,"","environment"],[1,2,1,"","idle_timeout"],[1,2,1,"","scheduler_cmd"],[1,2,1,"","scheduler_cores"],[1,2,1,"","scheduler_memory"],[1,2,1,"","worker_cmd"],[1,2,1,"","worker_cores"],[1,2,1,"","worker_memory"],[1,2,1,"","worker_threads"]],"dask_gateway_server.backends.local.UnsafeLocalBackend":[[1,2,1,"","api_url"],[1,2,1,"","backoff_base_delay"],[1,2,1,"","backoff_max_delay"],[1,2,1,"","check_timeouts_period"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_heartbeat_period"],[1,2,1,"","cluster_heartbeat_timeout"],[1,2,1,"","cluster_options"],[1,2,1,"","cluster_start_timeout"],[1,2,1,"","cluster_status_period"],[1,2,1,"","clusters_directory"],[1,2,1,"","db_cleanup_period"],[1,2,1,"","db_cluster_max_age"],[1,2,1,"","db_debug"],[1,2,1,"","db_encrypt_keys"],[1,2,1,"","db_url"],[1,2,1,"","inherited_environment"],[1,2,1,"","parallelism"],[1,2,1,"","sigint_timeout"],[1,2,1,"","sigkill_timeout"],[1,2,1,"","sigterm_timeout"],[1,2,1,"","stop_clusters_on_shutdown"],[1,2,1,"","worker_start_failure_limit"],[1,2,1,"","worker_start_timeout"],[1,2,1,"","worker_status_period"]],"dask_gateway_server.backends.yarn":[[1,0,1,"","YarnBackend"],[1,0,1,"","YarnClusterConfig"]],"dask_gateway_server.backends.yarn.YarnBackend":[[1,2,1,"","api_url"],[1,2,1,"","app_client_cache_max_size"],[1,2,1,"","backoff_base_delay"],[1,2,1,"","backoff_max_delay"],[1,2,1,"","check_timeouts_period"],[1,2,1,"","cluster_config_class"],[1,2,1,"","cluster_heartbeat_period"],[1,2,1,"","cluster_heartbeat_timeout"],[1,2,1,"","cluster_options"],[1,2,1,"","cluster_start_timeout"],[1,2,1,"","cluster_status_period"],[1,2,1,"","db_cleanup_period"],[1,2,1,"","db_cluster_max_age"],[1,2,1,"","db_debug"],[1,2,1,"","db_encrypt_keys"],[1,2,1,"","db_url"],[1,2,1,"","keytab"],[1,2,1,"","parallelism"],[1,2,1,"","principal"],[1,2,1,"","stop_clusters_on_shutdown"],[1,2,1,"","worker_start_failure_limit"],[1,2,1,"","worker_start_timeout"],[1,2,1,"","worker_status_period"]],"dask_gateway_server.backends.yarn.YarnClusterConfig":[[1,2,1,"","adaptive_period"],[1,2,1,"","cluster_max_cores"],[1,2,1,"","cluster_max_memory"],[1,2,1,"","cluster_max_workers"],[1,2,1,"","environment"],[1,2,1,"","idle_timeout"],[1,2,1,"","localize_files"],[1,2,1,"","queue"],[1,2,1,"","scheduler_cmd"],[1,2,1,"","scheduler_cores"],[1,2,1,"","scheduler_memory"],[1,2,1,"","scheduler_setup"],[1,2,1,"","worker_cmd"],[1,2,1,"","worker_cores"],[1,2,1,"","worker_memory"],[1,2,1,"","worker_setup"],[1,2,1,"","worker_threads"]],"dask_gateway_server.models":[[1,0,1,"","Cluster"],[1,0,1,"","User"]],"dask_gateway_server.options":[[1,0,1,"","Bool"],[1,0,1,"","Float"],[1,0,1,"","Integer"],[1,0,1,"","Mapping"],[1,0,1,"","Options"],[1,0,1,"","Select"],[1,0,1,"","String"]],"dask_gateway_server.proxy":[[1,0,1,"","Proxy"]],"dask_gateway_server.proxy.Proxy":[[1,2,1,"","address"],[1,2,1,"","api_token"],[1,2,1,"","externally_managed"],[1,2,1,"","gateway_url"],[1,2,1,"","log_level"],[1,2,1,"","max_events"],[1,2,1,"","prefix"],[1,2,1,"","proxy_status_period"],[1,2,1,"","tcp_address"],[1,2,1,"","tls_cert"],[1,2,1,"","tls_key"]],dask_gateway:[[0,0,1,"","Gateway"],[0,0,1,"","GatewayCluster"],[0,0,1,"","GatewayClusterError"],[0,0,1,"","GatewayServerError"]]},objnames:{"0":["py","class","Python class"],"1":["py","method","Python method"],"2":["py","attribute","Python attribute"]},objtypes:{"0":"py:class","1":"py:method","2":"py:attribute"},terms:{"0":[0,1,4,8,9,10,11,15],"0022336223893512945":15,"02":3,"03d":1,"04":3,"0m":3,"1":[0,1,4,10,11,13,15],"10":[1,10,15],"100":[1,8,10,15],"1000":15,"12":3,"127":[1,11],"146":[5,10,15],"148":[5,10,15],"15":1,"16":4,"160":10,"17":8,"187":[5,10,15],"19":3,"198":15,"1s":1,"2":[0,1,3,4,8,9,10,15],"20":[1,3,5],"202":[5,15],"2022":10,"21":3,"22":3,"23":3,"233":10,"24":[3,10],"245":10,"247":10,"29":3,"3":[1,3,4,10],"30":[1,4],"300":[1,10],"30304":10,"32":[1,2,10,14],"339":3,"35":[5,15],"352":3,"353":3,"360":3,"372":3,"374":3,"396":3,"399":3,"4":[1,4,8,9,10,15],"400":[2,8],"408":3,"410":3,"411":3,"413":3,"415":3,"416":3,"420":3,"423":3,"424":3,"425":3,"426":3,"429":3,"430":3,"431":3,"438":3,"441":3,"442":3,"443":3,"444":3,"445":3,"447":3,"448":3,"454":3,"460":3,"463":3,"464":3,"467":3,"468":3,"474":3,"477":3,"479":3,"481":3,"485":3,"488":3,"490":3,"491":3,"492":3,"493":3,"494":3,"495":3,"497":3,"499":3,"5":[1,3,8,9,10],"50":[1,10],"500":[3,15],"501":3,"502":3,"503":3,"505":3,"507":3,"508":3,"51":[10,15],"510":3,"512":3,"513":3,"514":3,"517":3,"518":3,"519":3,"521":3,"525":3,"527":3,"530":3,"531":3,"532":3,"533":3,"534":3,"536":3,"538":3,"540":3,"541":3,"545":3,"548":3,"549":3,"550":3,"551":3,"552":3,"554":3,"555":3,"556":3,"557":3,"558":3,"559":3,"561":3,"564":3,"567":3,"569":3,"572":3,"573":3,"575":3,"578":3,"58":[5,10,15],"580":3,"588":3,"590":3,"593":3,"595":3,"596":3,"597":3,"598":3,"599":3,"6":10,"60":1,"600":[1,3,8,9,10],"601":3,"602":3,"603":3,"606":3,"609":3,"611":3,"612":3,"616":3,"617":3,"620":3,"621":3,"623":3,"625":3,"627":3,"628":3,"630":3,"631":3,"634":3,"635":3,"636":3,"637":3,"639":3,"640":3,"644":3,"647":3,"652":3,"65252":15,"655":3,"656":3,"659":3,"660":3,"663":3,"664":3,"666":3,"667":3,"668":3,"669":3,"672":3,"673":3,"677":3,"68":[5,15],"685":3,"688":3,"690":3,"6c14f41343ea462599f126818a14ebd2":15,"6h":3,"6m54":10,"7":3,"700":3,"739":3,"741":3,"742":3,"748":3,"755":8,"766":3,"770":3,"771":3,"772":3,"773":3,"776":3,"778":3,"790":3,"791":3,"792":3,"794":3,"795":3,"799":3,"8":[1,3,4,10],"80":[10,13],"8000":[1,8,9,10,11],"8001":[8,9],"86400":[1,10],"87":[5,15],"8786":[5,10,15],"8787":10,"8788":10,"9000":10,"9s":8,"boolean":1,"byte":[1,4,14],"case":[1,8,9,10,14],"catch":10,"class":[0,4,5,10],"default":[0,1,2,3,4,7,8,9,10,11,13,14,15],"do":[1,2,7,8,9,10,11,14,15],"enum":1,"export":[8,9],"float":[1,4,10],"function":4,"import":[1,4,5,8,9,10,11,15],"int":[0,1,4],"long":[3,8,9],"new":[0,1,4,5,6,7,8,11,13],"null":[5,10],"public":[1,3,5,8,9],"return":[0,1,4,8,9,10,15],"short":9,"static":4,"super":1,"true":[0,1,5,10,14],"try":[8,9,10],"var":[8,9],A:[0,1,4,7,8,9,10,13],As:[2,10,14,15],At:[8,9,10,11,15],Being:2,By:[0,1,4,8,9,10,11,13,14],FOR:1,For:[1,2,4,5,8,9,10,14,15],If:[0,1,2,4,5,8,9,10,11,12,13,14,15],In:[6,7,8,9,13,14],It:[4,7,8,11,12,14,15],No:[0,2,3],Not:10,OR:[8,9],On:[1,4],One:[4,10,14],Or:[4,6,15],The:[0,1,2,3,4,5,6,7,8,9,10,12,14,15],Then:[2,6],There:[4,10,14],These:[1,10],To:[2,3,4,5,6,8,9,10,11,13,14,15],With:[2,10],_build:6,_proxi:5,abl:[8,9,10,15],about:[0,3,5,15],abov:[1,2,5,6,8,9,10,11,15],absolut:8,accept:8,access:[0,1,3,5,7,8,9,10,15],accomplish:[8,9],account:1,across:[1,2,3,7,9],act:10,action:[3,8],activ:[0,1,3,6,7,8,9,10,11,13,15],actual:[1,3],ad:[1,2,8,9,10,14],adapt:[0,1],adapt_clust:0,adaptive_period:1,add:[1,2,3,4,8,9,10],addit:[0,1,4,5,6,7],addition:10,additionalargu:10,additionali:9,addprinc:[2,8],address:[0,1,3,5,10,15],addus:[8,9],admin:[1,2,8,9,14],administr:[4,5,7,8,9,11,12,13,15],adopt:3,advis:11,affin:10,after:[1,4,6,10],afterward:0,ag:10,again:[2,3],against:3,aktech:3,alia:0,alic:8,align:3,all:[0,1,2,4,5,6,7,8,9,10,15],allow:[1,3,4,7,8,9,10,15],almost:1,along:[2,8],alongsid:[2,10],alphabet:10,alreadi:[2,8,10,15],also:[0,1,2,3,5,6,8,9,10,11,12,14,15],alter:8,altern:[9,15],alwai:[1,10,13],amanning9:3,amd64:3,amount:[1,13],an:[0,1,2,3,4,6,7,10,11,13,14],anaconda:[8,9],andreagiardini:3,ani:[0,1,4,5,8,9,10,11,13,14,15],annot:[1,10],anoth:[6,8],anti:10,anyon:8,anyth:[8,9],api:[1,2,3,8,9,10],api_address:1,api_token:[0,1,2,14],api_url:[1,2],apitoken:10,apitokenfromsecretkei:10,apitokenfromsecretnam:10,apiurl:10,apivers:10,app:1,app_client_cache_max_s:1,appeal:8,append:10,appli:[0,1,3,10],applic:[1,2,8,14],approach:8,appropri:[0,9],ar:[0,1,3,4,5,6,7,10,11,13,14,15],aravindrp:3,archiv:1,argument:[4,5,10,15],arm64:3,arokem:3,around:[1,8,10],arrai:15,artifact:3,asctim:1,aspect:10,assign:[8,9],associ:[1,3,10],assum:[6,15],async:[0,3,4],asynchron:0,asyncio:3,attach:0,attempt:1,attribut:[0,10,15],attributeerror:0,auth:[0,1,2,5,8,10,15],authent:[3,5,7,8,11,15],authenticator_class:[1,2,8],auto:3,autobuild:6,autodoc:6,autoformatt:3,autom:3,automat:[0,1,7,8,10,14,15],autoupd:3,avail:[0,1,4,8,9,10,13,15],averag:[1,10],avoid:[3,8,9,10],await:3,b:[1,8,9],back:[1,4,5,10],backend:[2,3,4,6,7,8,10],backend_class:[1,8,9],background:0,backoff:[1,10],backoff_base_delai:1,backoff_max_delai:1,backoffbasedelai:10,backoffmaxdelai:10,bad:3,badg:3,base64:1,base:[0,3,4,9,10,15],bash:[8,9],bashrc:[8,9],basic:[0,1,4,5,7,10,15],basicauth:0,batch:9,becom:3,been:[3,10,15],befor:[1,3,5,8,9],before_instal:3,beforehand:8,behavior:10,behind:14,being:[0,1,14],belong:1,below:[3,6,8,10],besid:6,best:[8,9],between:[1,2,4,7,8,9,10,14,15],bin:[1,8,9],bit:3,black:3,block:10,boilerpl:3,bokeh:3,bolliger32:3,bool:[0,1],bot:3,both:[1,6,7,8,9,10,11,14,15],bound:[0,1,3,4],branch:3,brew:3,broken:3,browser:[0,5,6],build:3,buildx:3,built:6,bump:3,bundl:3,c:[1,2,4,6,8,9,10,11,12,13,14],ca:1,cach:1,cache_max_ag:1,call:[0,1,4,5,8,9,10,15],callabl:1,calver:3,camelcas:[1,10],can:[0,1,2,4,5,6,7,8,9,10,11,12,13,14,15],cancel:1,cancel_command:1,capac:13,care:5,caselessstrenum:1,caus:[3,10],cd:[8,9],cdibbl:3,ce498e95403741118a8f418ee242e646:15,central:[3,7,8],cert:[1,14],certain:[4,8],certfic:1,certif:1,chang:[1,4,5,10,15],chart:3,chartpress:[3,10],check:[1,8,9,10,11,15],check_timeouts_period:1,chmod:[2,8],choic:[1,10,15],choldgraf:3,choos:10,chown:[2,8,9],chunk:[3,15],ci:3,classmethod:0,clean:[0,10,15],cleanli:15,cleanup:[1,3,10],cli:[1,3,8,9,14],click:3,client:[1,3,5,6,7,8,9,10,11,12,15],close:[0,15],close_rpc:3,cloud:[7,10],cluster:[0,3,5,7,9,11],cluster_config_class:1,cluster_heartbeat_period:1,cluster_heartbeat_timeout:1,cluster_max_cor:[1,13],cluster_max_memori:[1,13],cluster_max_work:[1,13],cluster_nam:0,cluster_opt:[0,1,4,5,9,10,15],cluster_start_timeout:[1,8,9],cluster_status_period:1,clusterconfig:[4,13],clusterip:10,clusteropt:10,clusterreport:[0,15],clusterrol:10,clusterrolebind:10,clusters_directori:1,clusterstatu:[0,1],cmnd_alia:9,code:[0,3,6,10],collect:8,color:3,com:[1,3,6,8,9,10,15],combin:1,come:3,comma:3,command:[1,2,3,6,8,9,10],comment:3,commit:3,common:[2,4,8,9,10,14],common_annot:1,common_label:1,commonli:[1,2],commun:[7,10,14],compani:10,compat:[3,10],compil:6,complet:[1,8,10,15],completed_cluster_cleanup_period:1,completed_cluster_max_ag:1,completedclustercleanupperiod:10,completedclustermaxag:10,complex:3,compon:[1,7,10],compos:[6,10,11,12],comput:1,concaten:10,conda:[1,3,8,9,11,12],condit:3,config:[1,3,5,8,9,10,14],config_fil:1,configur:[0,2,3,6,7,11,14],conflict:3,connect:[0,1,3,5,7,8,9],consequ:3,consid:6,consideratio:3,consist:[3,7],constrain:3,constructor:[0,4,5,15],consult:15,contact:[1,5,9,12],contain:[1,6,8,10],context:15,continuous_integr:6,contribut:3,control:[1,3,10],convers:4,convert:[1,4],cooki:1,cookie_nam:1,copi:1,copybutton:3,core:[1,3,10,13,15],correspond:[1,2,4],cost:8,could:[8,9],counter:1,coverag:3,cpu:1,crd:[1,3,10],crd_version:1,creat:[0,1,2,4,5,6,11,13,14],credenti:1,cryptograph:1,cryptographi:6,cslovel:3,ctrl:11,curl:[8,9],current:[0,1,9,15],custom:5,customiz:3,cut:3,d:[1,8,9],da:15,dashboard:[0,1,5,8,9,10],dashboard_address:1,dask:[0,1,2,3,4,5,11,12,14],dask_gatewai:[0,1,4,5,8,9,10,11,15],dask_gateway_config:[1,2,8,9,10,14],dask_gateway_encrypt_kei:1,dask_gateway_jobqueue_launch:[1,9],dask_gateway_proxy_token:[1,14],dask_gateway_serv:[1,2,4,8,9,10],dask_us:9,daskclust:[1,3,10],daskgatewai:[1,2,8,9,10],data:1,databas:1,date:1,db_cleanup_period:1,db_cluster_max_ag:1,db_debug:1,db_encrypt_kei:1,db_record_max_ag:1,db_url:1,de:3,deactiv:0,debug:10,declar:[1,3,4],decreas:1,deem:1,deep:[1,10],def:[1,4,9,10],defin:1,definit:3,del:3,delai:[1,3,10],delet:[1,3,10],delgadom:3,delimit:1,demo:[11,15],demonstr:8,dep:3,depedabot:3,depend:[3,4,8,9,10],dependabot:3,deploi:[1,2,3,7,10,11],deploy:[0,3,7,10,11,14,15],deprec:3,describ:[0,4,11,15],design:7,desir:[0,2,13,15],destin:3,detail:9,detect:[1,10],determin:[1,8,9],dev:[3,6],develop:[3,10,11],devenv:6,df:8,dgerlanc:3,dhirschfeld:3,dict:[0,1,4],dictionari:[1,4],didn:3,differ:[0,1,3,6,8,9],digit:3,direct:[1,3,6,7,11],directli:[10,15],directori:[1,3,6],disabl:[3,5,10,15],disclaim:3,discuss:[3,9],disk:9,dispar:9,displai:[3,10],disrupt:7,distribut:[0,1,3,8,15],divid:7,dn:10,doc:[2,3,6,8,9],docker:[1,3,4,6,15],dockerfil:3,docstr:0,document:[1,2,5,7,8,10,11],doe:14,doesn:[1,2,3,4,8,9,10],doesnotexist:10,domain:[2,8,14],don:[1,3,4,10,12,15],done:[2,4,8,9,10,11,12,15],doubl:3,douglasdavi:3,down:[1,15],download:[3,10],droctothorp:3,drop:3,dropdown:10,dummi:3,dump:1,duplic:3,durat:3,dure:8,dynam:4,e:[0,1,4,5,6,7,8,10,14,15],each:[0,1,2,4,6,7,8,9,10,13,15],eas:[7,8,9],easier:3,easiest:[8,9],easili:[2,6],echo:[8,9],ecosystem:7,edg:9,edit:6,effect:1,effici:9,either:[1,4,8,9,10,12,14,15],element:1,els:4,empti:[1,8,9,10,11],enabl:[1,2,10],encod:[1,14],encourag:[3,10],encrypt:[1,7,14],end:[4,6,10],enforc:7,entri:[9,10],entrypoint:[1,3],env:[1,3,6,8,9,10],envfrom:1,environ:[1,2,3,4,5,7,10,13,14,15],epoch:1,equal:[1,7],equival:1,erl987:3,error:[0,3,4,5,8,9,15],especi:5,etc:[0,2,4,5,7,8,9,10,14,15],event:1,ever:1,everi:[1,5,8,9],everyth:[8,9,11,15],exampl:[0,1,3,5,10,15],exce:1,exceed:15,excel:10,execut:[1,3,9],exempt:1,exist:[2,3,7,8,10,11],exit:15,expect:[1,4],experi:7,experiment:11,explicit:3,explicitli:[0,3,4,5,10,14,15],expos:[1,8,9,10],extend:3,extern:[1,10],externally_manag:[1,14],extra:[1,3,10],extract:8,f:[3,8,9],face:[0,1,2,4,5,8],fail:[0,1,3],failov:7,failur:[1,3,7,10],failurethreshold:10,fall:5,fals:[0,1,3,5,10,15],fanshi118:3,feedback:1,few:[1,2,8,9,10,13,14,15],fg:10,field:[0,1,2,3,4,5,8,9,10,14,15],file:[1,2,3,4,5,8,9,10,14],filesystem:1,fill:9,find:10,fine:[10,11],finer:1,first:[1,2,3,6,8,9,10],fit:[2,8],fixm:3,fjetter:3,flag:3,flake8:3,flexibl:7,folder:[6,8],follow:[1,2,3,4,5,8,9,10,14],forc:3,forg:[6,8,9,11,12],fork:3,form:[1,2,10],format:[1,3,5],format_byt:3,formatt:1,forward:[1,4,10],fqdn:[2,8,14],free:[8,9],frequent:1,friendli:4,from:[0,1,2,3,4,5,6,8,9,10,11,15],from_nam:0,frozen:3,full:[0,1,3,5,6,8,9,10,15],fullchain:14,fulli:[2,8,10,14],fullnameoverrid:[3,10],further:[2,8,9,14],g:[0,1,4,5,7,8,9,10,14,15],gatewai:[2,3,4,5,12,14],gateway_hostnam:1,gateway_inst:1,gateway_url:1,gatewayauth:0,gatewayclust:[3,4,5,8,9,15],gatewayclustererror:0,gatewayservererror:0,gener:[1,2,6,10,14],generate_opt:4,georgianaelena:3,get:[0,7,10],get_client:[0,8,9,15],get_clust:0,get_vers:0,gh:3,ghcr:[3,10],gib:[1,4,10],gibibyt:1,giffel:3,git:6,github:[3,6],githubusercont:3,gitignor:3,give:1,given:[0,10],gke:10,global:[1,10],go:[0,3,6,10],goe:10,golang:3,good:15,googl:10,got:15,greater:1,group1:8,group2:8,group:[1,8],gui:[1,4],guid:10,gz:[1,8],h:1,ha:[1,3,8,9,10,15],had:3,hadoop:[1,3,6,7],handl:10,handler:[1,4,9,10],hang:3,happen:10,have:[0,1,3,4,7,8,9,10,14,15],hdf:[1,8],heartbeat:[1,10],heavi:7,held:5,helm:3,help:[1,3],here:[1,2,4,6,8,9,10,13,14,15],hex:[1,2,10,14],hide:10,high:[8,9,10],higher:1,highli:1,highlight:10,hold:1,holzman:3,home:[1,10],hook:3,host:[0,1,2,5,8,10],hostnam:1,hour:10,how:[1,4,6,8,9,10,11],howev:4,hpc:7,html:[1,6,10],http:[0,1,2,3,5,6,7,8,9,10,11,14,15],hub:[2,3,10],human:[1,4],i:[5,14],id2ndr:3,id:1,idea:3,ident:[8,9],identifi:[1,4,15],idl:1,idle_timeout:1,ifnotpres:3,ignor:1,imag:[1,3,4,15],image_pull_polici:1,image_pull_secret:1,imagepullpolici:3,imagepullsecret:[3,10],impact:3,imperson:[2,8],implement:[1,2],improv:1,inbound:10,includ:[3,8,9,10,15],inclus:1,incom:10,increas:[3,8,9,10],increment:1,indic:0,indirect:3,individu:[1,4,9],infer:[5,10],infin:0,info:[0,1,10],inform:[0,1,2,4,5,6,8,9,10,11,13,15],ingressrout:[1,3],ingressroutetcp:1,inherit:1,inherited_environ:1,init:10,initialdelaysecond:10,inject:1,insecur:[1,11],insid:1,inspect:[6,10],instal:[0,3,15],instanc:[0,1,10,11],instanti:5,instead:[1,3,4,11,15],institut:10,instruct:[3,8,9,10],integ:[1,4,10],intend:[8,9],interact:[2,8,9,10,12,15],interest:9,interfac:[0,1],intermitt:3,intern:[0,1],interpret:4,invalid:[0,3],io:[3,10],ioloop:0,ip:[0,5,10],ipywidget:[0,3,4,15],isn:[8,9,10],isol:11,isort:3,issu:[3,9],item:10,iter:3,its:[8,9,10],itself:10,iu:[8,9],jacobtomlinson:3,james:3,jcoll88:3,jcrist:3,jcristharif:1,job:[3,7,8],jobqueu:[1,9],jobqueueclusterbackend:9,jobqueueclusterconfig:9,joejasinski:3,jrbourbeau:3,jsignel:3,json:[1,3,10],jupyt:[0,3,7,15],jupyterhub:[0,1,3,5,7,15],jupyterhub_api_token:1,jupyterhub_api_url:1,jupyterhub_config:2,jupyterhubauth:0,jupyterhubauthent:2,jupyterlab:15,just:[1,2],juypterhub:10,k3:3,k3d:3,k8:[1,3,10],k8s_api_rate_limit:1,k8s_api_rate_limit_burst:1,k8sapiratelimit:10,k8sapiratelimitburst:10,k:[1,2,8],kadmin:[2,8],keep:[1,10],kei:[0,1,3,4,10,14,15],kept:14,kerbero:[0,1,5,7,10,15],kerberosauth:0,kerberosauthent:[2,8],keytab:[1,2,8,10],keyword:[1,4,5,15],kibibyt:1,kill:1,kind:10,kinit:8,kirill888:3,klucar:3,know:[12,15],kube:[3,10],kubeclusterconfig:[4,10],kubectl:[3,10],kubernet:[3,4,6,7],kubernetes_asyncio:6,kwarg:[0,1,5],label:[1,3,4,9,10],label_selector:1,lack:1,lacroix:3,lag:1,lambda:4,larg:[1,4,10,15],larger:1,last:[0,4,8,9,15],latenc:1,later:[0,15],latest:[1,3,8,9,10],launch:[1,7,9,10,15],launcher:[1,9],lead:[3,10],learn:8,least:[8,9],leav:[1,10,15],legaci:3,let:3,letsencrpyt:14,level:[1,3,9,10],levelnam:1,librari:[3,8,9,10,11,12,15],lifetim:15,lift:7,like:[2,3,8,9,10,14],limit:[1,8,9,10,15],line:[2,8],linger:15,link:[0,3,5],linkcheck:3,linux:[3,8,9],list:[0,1,3,6,8,9,10,11],list_clust:[0,8,9,10,11,15],listen:[1,10],live:14,livenessprob:10,ll:[2,6,8,9,10,11,12,14],load:[1,8,9,15],loadbalanc:10,local:[0,3,6,7,9,10,15],localize_fil:[1,8],locat:[2,6,8,9,14],log:[1,3,10],log_color:1,log_datefmt:1,log_format:1,log_level:1,loglevel:10,logo:3,longer:[3,15],look:[8,9,10],lookup:10,loop:[0,3],looser:8,lose:7,low:1,lower:3,luna:3,m:1,mac:3,magnitud:1,mai:[1,4,5,8,9,10,11,13,15],main:[3,6],maint:3,maintain:[3,10],make:[1,2,3,4,6,7,8,9,10],manag:[6,7,8,9,10,11,14,15],mani:[3,7,10,13],manner:10,manual:[1,8,9,15],map:[0,1,4,10],margin:2,mark:[1,3],markdown:3,martindur:3,master:3,match:[1,10,12],matchexpress:10,materi:10,matter:[8,9,10],max:[1,4,10,13,15],max_ev:1,maxime1907:3,maximum:[0,1,10,13,15],md:3,mean:[10,15],meaningless:10,mebibyt:1,mechan:8,medium:4,meet:10,mem:1,member:4,memori:[1,3,10,13,15],memorylimit:1,menend:3,merg:[1,10],messag:[1,3,13],metadata:10,method:[0,1,2,7,11,15],micro:3,middlewar:1,might:[8,9],min:[1,4,8,9,10],miniconda3:[8,9],miniconda:[8,9],minikub:6,minim:[3,10],minimum:[0,1,8,9,15],minut:[8,9,10],misc:3,miss:3,mkdir:[8,9],mm:3,mmccarti:3,mode:0,model:[4,8],modern:3,modifi:0,modul:3,month:3,monthli:3,more:[0,1,2,4,5,7,8,9,10,11,15],most:[0,3,8,10,15],mount:9,mountfailur:10,move:[3,8],ms:1,msec:1,mukheri:3,multi:[2,3,7,10],multipl:[1,3,7,8,10],must:[1,3,4,10,14,15],mutabl:0,mutablemap:15,my:[1,10,14],myenv:10,mygatewai:15,mypassword:2,myst:3,myvalu:0,n:[0,6,8,9],name:[0,1,2,3,4,8,9,10,14,15],nameoverrid:[3,10],namespac:[1,3,10],nativ:[1,3,7,10],ncpu:1,necessari:[5,14,15],necessarili:1,need:[1,2,3,4,5,6,8,9,10,11,12,14,15],nest:10,network:[1,3,10],new_clust:[0,4,5,8,9,15],newli:[8,9],next:[8,9],nf:9,nginx:14,nice:15,node:[1,8,9,10],nodeaffin:10,nodeport:10,nodeselector:[3,10],nodeselectorterm:10,none:[0,1,5,10],nopasswd:9,norandkei:[2,8],normal:[0,1,11,15],noschedul:1,not_a_valid_opt:0,note:[1,2,3,4,5,6,8,9,10,11,14,15],notebook:[0,2,4,10,15],now:[3,5,8,9,10],num_failur:1,number:[0,1,10,13],numer:15,numpi:3,o:[8,9,10],object:[0,1,4,5,8,9,10,15],observ:10,obtain:14,occat:3,odd:10,offer:[4,10],old:[1,3],older:[1,3],olivi:3,on_cluster_heartbeat:1,onc:[10,11,15],one:[0,2,3,7,8,9,10,15],onli:[0,1,2,3,5,8,9,10,11,12,14,15],onlin:10,onward:3,open:10,openssl:[1,2,10,14],oper:[0,1,7,9,10,15],opt:[8,9],optim:3,option:[2,3,5,7,11,14,15],option_handl:10,options_handl:[1,4,9],order:[1,10],org:[3,10],organ:[3,7],other:[0,1,2,8,10,15],otherwis:0,our:[3,8,9,15],out:[0,10],outdat:3,output:[2,11],over:10,overhead:1,overrid:[0,1,4],own:[3,8,9,10],p:[8,9],pack:[1,8],packag:[3,6,8,9,11,12],page:[3,6,10,11],pair:[10,14],panda:8,parallel:1,paramet:[0,1,4,5,15],parser:3,part:9,partial:10,particular:[6,7,10],partit:1,pass:[0,4,15],password:[0,1,2,5,10],passwordless:9,path:[1,5,8,10,14],patrix58:3,pb:[1,3,6,7,9],pbsbackend:9,pbsclusterbackend:9,pbsclusterconfig:9,peak:3,pem:14,pend:0,peopl:3,pep600:3,pep:3,per:[1,7,10,13],perfectli:11,perform:[1,8,9,15],period:1,periodsecond:10,permiss:1,persist:[0,15],ph0tonic:3,pin:3,pip:[3,11,12],place:3,plan:9,platform:3,pleas:[6,9],plenti:10,pluggabl:7,plugin:7,pod:[1,3,10],podspec:10,point:[3,8,9,10,11,15],polici:1,port:[0,1,2,5,10],possibl:14,potenti:[7,10],power:4,practic:15,pre:3,preemptibl:10,prefer:10,preferenti:15,prefix:[1,10],premis:7,prepend:1,presenc:1,present:14,prevent:[1,4],previous:3,princip:[1,2,8],privat:1,privkei:14,probabl:[1,8,9],procedur:10,process:[3,4,7,8,9,10,11,15],product:[2,14],profil:3,programat:0,programmat:[1,4,15],project:[1,3,10],prone:5,properli:[8,9,11,15],properti:8,protocol:[2,7],provid:[0,1,2,3,4,5,6,7,8,9,10,15],proxi:[0,3,5,7,9,10,15],proxy_address:[0,1,8,9,10,15],proxy_prefix:1,proxy_status_period:1,proxy_tcp_entrypoint:1,proxy_web_entrypoint:1,proxy_web_middlewar:1,proxyus:8,public_address:0,publish:3,pull:[1,10],pullpolici:10,purg:3,purpos:[2,3,10,11],push:3,put:[8,10],py39:3,py:[1,2,3,8,9,10,14],pypa:3,pypi:3,pyproject:3,pytest:[3,6],python:[1,3,4,6,10],q:[2,8],qemu:3,qmgr:9,qo:1,qualifi:[2,8,14],quasiben:3,queri:[4,11,15],queue:[7,8],quickli:0,quickstart:15,rais:[0,3,4,15],ram:13,rand:[1,2,10,14],randkei:[2,8],random:[1,10,14,15],rang:4,rather:10,raw:3,raybellwav:3,rbac:10,re:[6,8,9,10,11,13,15],readabl:[1,2,4,8,14],readi:[8,9,10],readinessprob:10,readm:3,real:11,reason:[10,14],reauthent:1,rebrand:3,rebuild:6,receiv:[1,4,10],recent:[0,15],recommend:[2,5,6,8,9,10,15],reconcil:[1,3],reconnect:15,record:[1,10],reduc:[1,3,8,15],refactor:3,refer:[3,6,8,9],referenc:1,refesh:1,refreez:3,refresh:[3,6],regardless:4,regist:[1,2,3,10],registri:[1,3],regress:3,regular:6,relat:[0,3,10],releas:10,relev:14,reli:[1,10],reloc:3,relocat:8,remedi:13,remov:[1,3,10],reorder:3,repeat:8,replac:[2,10],replica:10,repo:[3,8,9,10],report:0,repositori:3,repres:4,represent:[1,4],request:[0,1,2,3,4,8,9,10,13],requir:[1,3,6,7,8,9,10,15],requiredduringschedulingignoredduringexecut:10,reset:1,resolv:3,resourc:[1,3,4,8,9,10,15],resourcevers:10,respect:[7,10],respond:1,respons:[1,9],rest:[8,9],restart:7,restrict:8,result:[1,6],retain:1,retri:[1,10],revert:3,review:3,rigzba21:3,rileyhun:3,rm:[3,8,9],robust:[3,7],role:10,rolebind:10,root:[0,5,8,9],rotat:1,round:3,rout:[1,3,10],rsignel:3,rst:3,run:[0,1,2,3,4,7,8,9,10,11,12,14],runtim:[1,8,9],s:[0,1,3,4,5,7,9,10,11,14,15],sai:13,same:[0,1,6,7,8,10,11],sampl:3,scalabl:10,scale:[0,1,5,10],scale_clust:0,scenario:10,scharlottej13:3,schedul:[0,1,3,5,7,8,9,10,15],scheduler_address:1,scheduler_cmd:[1,8,9],scheduler_cor:1,scheduler_cores_limit:1,scheduler_extra_container_config:[1,10],scheduler_extra_pod_annot:1,scheduler_extra_pod_config:[1,10],scheduler_extra_pod_label:1,scheduler_memori:1,scheduler_memory_limit:1,scheduler_resource_list:1,scheduler_setup:[1,8,9],schema:[3,10],scheme:11,scikit:8,script:[1,3,6,8,9],seamless:7,sebastian:3,second:[1,3,8,9,10],secondli:10,secret:[1,10,14],secretref:1,section:[10,11],secur:[2,7],see:[0,1,2,3,4,5,6,7,8,9,10,11,15],seem:3,select:[0,1,4],selector:1,selflink:10,send:10,sens:7,sent:1,separ:[1,7,10],sequenc:1,serial:1,serv:[1,8,9,10],server:[0,2,3,5,6,7,10,12,14],servic:[1,2,3,7,8,10],service_nam:1,serviceaccount:10,serviceaccountnam:10,session:15,set:[0,1,2,3,4,5,6,8,9,10,13,15],set_as_default:0,setup:[1,3,6,9,10,11,15],setuptool:3,sever:[4,8,9],sh:[3,8,9],share:[2,5,7,8,10,13],shell:1,should:[0,1,4,8,9,10,11,12,14,15],shouldn:1,show:10,show_config:1,show_config_json:1,shut:1,shutdown:[0,1,3,7,8,9],shutdown_on_clos:[0,15],side:[0,3,4,15],sigint:1,sigint_timeout:1,sigkil:1,sigkill_timeout:1,signatur:[1,4],signific:3,sigterm:1,sigterm_timeout:1,similar:[3,4],simpl:[1,10,11],simpleauthent:[2,11],simpler:15,simpli:7,simplifi:[3,5],sinc:[1,4,8,9,14],singl:[1,3,8,9,10],size:[1,15],skaffold:3,skein:[1,6,8],skip:15,sleep:3,slightli:3,slow:[8,9],slower:1,slurm:[1,3,6,7,9],slurmbackend:9,small:[1,4,8,9],smaller:1,snake_cas:10,snippet:10,so:[1,2,8,15],softwar:[8,9],some:[1,3,8,10,15],someth:9,somewher:15,sooner:1,sort:10,sourc:[1,6,8,9],spec:[1,10],specif:[0,1,6,8,9,15],specifi:[1,4,5,10,14,15],sphinx:6,sphinx_copybutton:3,sqlalchemi:[3,6],sqlite:1,stage:1,staging_directori:1,standard:4,start:[0,1,3,10,15],start_clust:1,start_tim:1,startup:[3,7,8,9],state:0,statu:[0,1,9,15],status:0,status_command:1,stdout:1,still:2,stop:[0,1,9],stop_clust:[0,1],stop_clusters_on_shutdown:1,stop_tim:1,store:[1,2,8,9,14,15],str:[0,1],straight:10,strict:[8,9],strictli:[10,15],string:[1,5,9,10,14],style:3,subclass:1,subdirectori:[1,6],submiss:1,submit:[0,1],submit_command:1,substitut:10,successfulli:[1,8,9,10],sudo:[8,9],sudoer:9,suffix:1,suggest:10,suit:6,suitabl:2,summari:[8,9],supervisord:[1,8,9],support:[1,3,4,7,8,9,10,15],sure:[2,8,10],syntax:[1,3,8,9],system:[6,7,8,9],t:[1,2,3,4,8,9,10,12,13,15],tag:10,take:[4,8,9,10,14,15],taken:[0,5],tar:[1,8],target:[1,4],task:[1,8,9,10],tbump:3,tcp:[1,8,9,10],tcp_address:[1,8,9],tebibyt:1,technolog:7,templat:[1,5,9],temporari:1,tenant:[2,7],tensorflow:[4,15],termin:[3,11],test:[1,3,8,9,10,11,15],test_helm:3,than:[1,7,10,15],thei:[8,9,10,11,13,14],them:[3,4,9,10],theme:[3,6],themselv:[9,10],thi:[0,1,2,4,5,6,8,9,10,11,12,13,14,15],thing:[2,5,10,15],those:[1,4,10],though:11,thread:[1,3,10],three:[3,7,8,9],through:[8,9,10,14,15],throughout:8,thu:9,tib:13,time:[1,3,5,10],timeout:[1,3,8,9],timeouterror:3,timeoutsecond:10,tini:[3,10],tl:[1,5,7,15],tls_ca:1,tls_cert:[1,14],tls_kei:[1,14],tmp:[8,9],togeth:10,token:[0,1,2,10],toler:[1,3,10],tomaugspurg:3,toml:3,too:[1,3,4],tool:8,top:6,tornado:6,toward:10,traceback:[0,15],track:9,tradition:5,traefik:[1,3,10],traffic:[3,8,9,10],trait:6,traitlet:[3,6,7],transfer:9,translat:4,transpar:1,travi:3,trigger:3,truli:3,tupl:1,two:[4,6,10,11,12,14,15],txt:3,type:[0,1,4,5,8,9,10],typic:[8,9,15],typo:[3,10],ubuntu:3,udeet27:3,ui:[7,10],unarchiv:1,unbound:1,unchang:[1,3,4],under:[1,8,9,10],underli:[7,10],unexpos:10,unicod:1,uninstal:3,union:1,uniqu:[1,15],unnecessari:10,unpin:3,unrecogn:3,unsafelocalbackend:11,unset:[1,5],until:0,up:[0,1,3,6,8,9,10],updat:3,upgrad:[3,7,10],upload:[3,8],upon:15,url:[1,2,10,11],us:[0,1,3,4,5,6,7,11,12,15],usag:[3,5,11],use_local_default:0,use_stagein:1,user:[0,2,3,5,7,10,11,12,13,14,15],usernam:[0,1],usg:3,usr:[8,9],usual:[1,2,8,11],v1:10,v1alpha1:1,v1contain:1,v1podspec:1,v2:3,v63:3,v:6,valero:3,valid:[0,1,2,4,10,15],valu:[0,1,3,4,5,8,9,10,15],valueerror:15,variabl:[1,5,10,14],variou:6,ve:[8,15],venv:[1,8],verifi:[1,10],version:[0,1,3,9,10,12],version_info:0,via:[0,1,3,10,15],view:6,viniciusdc:3,virtual:[1,8,9],virtualenv:8,visibl:[1,8,14],wai:[8,9,14],wait:[0,1,3],walk:[8,9,15],want:[2,8,9,10,11,13,14,15],warn:[1,3,10,13],watch:1,wdhow:3,we:[1,2,4,5,6,8,9,10,11,13,14,15],web:[0,1,5,7,8,9,10,14],websit:6,weekli:3,well:[3,4,7,8,10,15],what:[3,4,7,15],wheel:3,when:[0,1,2,3,4,5,8,9,10,11,15],where:[0,1,2,3,4,8,10,14],wherev:[2,8],whether:[0,1,4,10],which:[0,1,4,5,6,8,9,10,15],whitelist:1,whitespac:3,widget:[0,3,15],wildcard:8,wish:[4,15],within:10,without:[5,7,8,9,10,11],won:3,work:[1,3,4,10,15],workaround:3,worker:[0,1,3,8,9,10,13,15],worker_cmd:[1,8,9],worker_cor:[0,1,4,8,9,10,15],worker_cores_limit:1,worker_extra_container_config:[1,10],worker_extra_pod_annot:1,worker_extra_pod_config:[1,10],worker_extra_pod_label:1,worker_memori:[1,4,8,9,10,15],worker_memory_limit:1,worker_resource_list:1,worker_setup:[1,8,9],worker_start_failure_limit:1,worker_start_timeout:[1,8,9],worker_status_period:1,worker_thread:[1,3,10],workflow:[3,5,6,15],workload:15,world:11,would:2,writabl:8,written:[6,8,9],wrong:0,wstagein:1,x86_64:[8,9],x:3,xst:[2,8],y:[1,8,9],yaml:[3,5,6,10],yarn:7,yarnbackend:8,yarnclusterconfig:8,ye:3,yet:10,you:[1,2,3,4,5,6,7,8,9,10,11,12,14,15],your:[2,3,4,5,6,7,8,9,10,12,15],your_realm:8,yuvipanda:3,yyyi:3,z2jh:3,zero:[1,3,10],zip:1,zombi:1,zonca:3},titles:["Client API","Configuration Reference","Authentication","Changelog","Exposing Cluster Options","Configuration","Development","Dask Gateway","Install on a Hadoop Cluster","Install on an HPC Job Queue","Install on a Kubernetes Cluster","Install Locally (Quickstart)","Installation","Cluster Resource Limits","Security settings","Usage"],titleterms:{"0":3,"1":3,"10":3,"11":3,"2022":3,"2023":3,"2024":3,"4":3,"6":3,"9":3,"break":3,"class":1,"default":5,"new":[3,15],account:[8,9],ad:3,adapt:15,addit:[8,9,10],address:[8,9],an:[8,9,15],api:0,ar:[8,9],architectur:[7,10],archiv:8,authent:[0,1,2,10,12,14],backend:[1,9],base:1,bug:3,build:6,certif:14,chang:3,changelog:3,chart:10,client:0,clone:6,cluster:[1,4,8,10,13,15],clusterconfig:1,comput:15,conda:6,configur:[1,4,5,8,9,10,15],connect:[10,11,15],continu:3,contributor:3,core:4,creat:[8,9,10,15],custom:10,dask:[6,7,8,9,10,15],depend:[6,12],develop:6,differ:4,directori:[8,9],document:[3,6],down:10,enabl:[8,9,14,15],enhanc:3,environ:[6,8,9],everyth:10,exampl:[4,8,9,13],except:0,exist:15,experi:4,expos:4,extern:14,extraconfig:10,extracontainerconfig:10,extrapodconfig:10,featur:3,file:6,fix:[3,9],gatewai:[0,1,6,7,8,9,10,11,15],gatewayclust:0,group:4,hadoop:8,helm:10,highlight:[3,7],hpc:9,imag:10,improv:3,instal:[6,8,9,10,11,12],integr:[3,8],interact:11,job:[1,9],jupyterhub:[2,10],jupyterhubauthent:1,kerbero:[2,8,12],kerberosauthent:1,kubebackend:1,kubeclusterconfig:1,kubecontrol:1,kubernet:[1,10],letsencrypt:14,limit:13,local:[1,8,11],localbackend:1,localclusterconfig:1,made:3,mainten:3,manag:1,memori:4,merg:3,model:1,open:[8,9],option:[0,1,4,6,8,9,10,12],other:3,overview:7,own:14,path:9,pbsbackend:1,pbsclusterconfig:1,per:4,permiss:[8,9],pip:6,port:[8,9],pr:3,process:1,profil:4,proxi:[1,8,14],python:[8,9],queue:[1,9],quickstart:11,refer:[1,10],releas:3,relev:[8,9],repositori:6,resourc:13,run:[6,15],s:[2,8],scale:15,secur:[8,14],server:[1,4,8,9,11,15],set:14,shut:10,shutdown:[11,15],simpl:2,simpleauthent:1,slurmbackend:1,slurmclusterconfig:1,specif:4,specifi:[8,9],start:[8,9,11],termin:14,test:[2,6],thi:3,thing:[8,9],tl:14,token:14,unreleas:3,unsafelocalbackend:1,up:15,upkeep:3,us:[2,8,9,10,14],usag:15,user:[1,4,8,9],valid:[8,9],via:6,work:[8,9],worker:4,yarn:[1,8],yarnbackend:1,yarnclusterconfig:1,your:14}})