
 val data=sc.textFile("hdfs://localhost:8020/sparkdata/input/calllogdata")
 
 val drop=data.filter(res=>res.contains("DROPPED"))
 
 val SUCCESS=data.filter(res=>res.contains("SUCCESS"))
 
 val FAILED=data.filter(res=>res.contains("FAILED"))