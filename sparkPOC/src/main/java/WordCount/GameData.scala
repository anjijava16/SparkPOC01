

val data1= sc.textFile(""hdfs://localhost:8020/gamedata")

val data2=data1.map(_.split("\t"))

val data3= data2.map{ res=>
    	val name=res(0)
    	val country=res(1)
        val year =res(2)
        val sports=res(3)
       (name,country,year,sports)
}

data3.saveAsTextFile("/sparkdata/gamedata")
