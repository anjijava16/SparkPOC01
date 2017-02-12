 val data1=sc.textFile(""hdfs://localhost:8020/olympic_data.tsv")

val data2=data1.map(res=>res.split("\t"))

val size_4=data2.filter(res=>res.size==4)
val size_5=data2.filter(res=>res.size==5)
val size_6=data2.filter(res=>res.size==6)


val res_4=size_4.map{ res=>
  val name=if(res(2)!="")res(2) else "NOT KNOWN"
 val country= if(res(3)!="") res(3) else "NOT KNOWN"
  val year= "NOT KNOWN"
  val sports="NOT KNOWN"
  (name,country,year,sports)
  }
  
val res_5=size_5.map{ res=>
  val name=if(res(2)!="")res(2) else "NOT KNOWN"
 val country= if(res(3)!="") res(3) else "NOT KNOWN"
  val year= if(res(4)!="") res(4) else "NOT KNOWN"
  val sports="NOT KNOWN"
  (name,country,year,sports)
  }


val res_5=size_5.map{ res=>
  val name=if(res(2)!="")res(2) else "NOT KNOWN"
 val country= if(res(3)!="") res(3) else "NOT KNOWN"
  val year= if(res(4)!="") res(4) else "NOT KNOWN"
  val sports= if(res(5)!="") res(5) else "NOT KNOWN"
  (name,country,year,sports)
  }


 val result=res_6.union(res_5.union(res_4))
 
 // Sink to One partiton
  val res=result.coalesce(1)
  
 res.saveAsTextFile("/sparkdata/olympic_1")
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  


















