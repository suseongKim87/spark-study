 "아파치 스파크 입문, 따라 하며 쉽게 익히는 스파크 SQL, 스트림처리, 머신러닝 - 사루타 고스케, 도바시 마사루, 요시다 고요, 사사키 도루, 쓰즈키 마사요시" 
 
 5장의 예제를 풀어 따라해봤습니다.
 
 스파크의 기본적인 내용을 담고 있어서 나름 괜찮은 예제인것 같습니다. 
 
 간단한 설명은 코드 중간중간에 설명 되어 있습니다. 
 
#### sales-october.csv
```{text}
5830,2018-10-02 10:20:38,16,28
5831,2018-10-02 15:13:04,15,22
5832,2018-10-02 15:21:53,2,10
5833,2018-10-02 16:22:05,18,13
5834,2018-10-06 12:04:28,19,18
5835,2018-10-06 12:54:13,10,18
5836,2018-10-06 15:43:54,1,8
5837,2018-10-06 17:33:19,10,22
5838,2018-10-11 10:28:00,20,19
5839,2018-10-11 15:00:32,15,3
5840,2018-10-11 15:06:04,15,14
5841,2018-10-11 15:45:38,18,1
5842,2018-10-11 16:12:56,4,5
5843,2018-10-13 10:13:53,3,12
5844,2018-10-13 15:02:23,15,19
5845,2018-10-13 15:12:08,6,6
5846,2018-10-13 17:17:20,10,9
5847,2018-10-18 11:08:11,15,22
5848,2018-10-18 12:01:47,3,8
5849,2018-10-18 14:25:25,6,10
5850,2018-10-18 15:18:50,10,16
5851,2018-10-20 13:06:00,11,21
5852,2018-10-20 16:07:04,13,29
5853,2018-10-20 17:29:24,5,4
5854,2018-10-20 17:47:39,8,17
5855,2018-10-23 10:02:10,2,24
5836,2018-10-23 11:22:53,8,19
5857,2018-10-23 12:29:16,7,7
5858,2018-10-23 14:01:56,12,26
5859,2018-10-23 16:09:39,8,13
5860,2018-10-23 17:26:46,8,19
```

#### sales-november.csv
```{text}
5861,2018-11-01 10:47:52,15,22
5863,2018-11-01 11:44:54,8,26
5864,2018-11-01 14:29:51,18,10
5865,2018-11-01 17:50:00,6,17
5867,2018-11-04 10:03:57,15,16
5868,2018-11-04 11:22:55,15,13
5869,2018-11-04 16:32:09,19,6
5870,2018-11-10 11:12:30,17,27
5871,2018-11-10 13:32:53,17,13
5872,2018-11-10 15:31:21,4,15
5873,2018-11-10 16:03:01,6,5
5874,2018-11-10 17:52:20,12,28
5875,2018-11-15 11:36:39,3,5
5876,2018-11-15 14:08:26,9,7
5877,2018-11-15 15:05:21,10,0
5878,2018-11-18 11:17:09,7,16
5879,2018-11-18 14:50:37,9,3
5880,2018-11-18 16:23:39,4,20
5881,2018-11-18 17:28:31,18,25
5882,2018-11-22 10:50:24,7,26
5883,2018-11-22 11:43:31,3,3
5884,2018-11-22 12:57:22,4,12
5885,2018-11-22 15:20:17,19,25
5886,2018-11-25 16:42:07,10,27
5887,2018-11-25 17:38:03,14,0
5888,2018-11-25 18:30:36,10,8
5889,2018-11-25 18:41:57,11,10
5890,2018-11-30 14:30:08,11,17
5862,2018-11-30 14:57:47,8,22
5866,2018-11-30 15:17:29,8,24
```
#### products.csv
```{text}
0,송편(6개),12000
1,가래떡(3개),16000
2,연양갱,5000
3,호박엿(6개),16000
4,전병(20장),4000
5,별사탕,3200
6,백설기,3500
7,약과(5개),8300
8,강정(10개),15000
9,시루떡,6500
10,무지개떡,4300
11,깨강정(5개),14000
12,수정과(6컵),19000
13,절편(10개),15000
14,팥떡(8개),20000
15,생과자(10개),17000
16,식혜(2캔),21000
17,약식,4000
18,수수팥떡(6개),28000
19,팥죽(4개),16000
20,인절미(4개),10000
```

#### scala code
```{.scala}
import org.apache.spark.rdd.RDD

def createSalesRDD(csvFile: String) = {
	val logRDD = sc.textFile(csvFile)
	logRDD.map { record =>
		val splitRecord = record.split(",")
		val productId = splitRecord(2)
		val numOfSold = splitRecord(3).toInt
		(productId, numOfSold)
	}
}

def createOver50SoldRDD(rdd: RDD[(String, Int)]) = {
	rdd.reduceByKey(_+_).filter(_._2 >= 50)
}

val salesOctRDD = createSalesRDD("/data/sales-october.csv")
val salesNovRDD = createSalesRDD("/data/sales-november.csv")

val octOver50SoldRDD = createOver50SoldRDD(salesOctRDD)
// octOver50SoldRDD.collect.foreach(println)
/*
결과값
(8,68)
(15,80)
(10,65)
*/
val novOver50SoldRDD = createOver50SoldRDD(salesNovRDD)
// novOver50SoldRDD.collect.foreach(println)
/*
(8,72)
결과값
(15,51)
*/

//join
val bothOver50SoldRDD = octOver50SoldRDD.join(novOver50SoldRDD)
// bothOver50SoldRDD.collect.foreach(println)
/*
결과값
(8,(68,72))
(15,(80,51))
*/

//join된 데이터 끼리 합산
val over50SoldAndAmountRDD = bothOver50SoldRDD.map {
	case (prodjctId, (octAmount, novAmount)) =>
		(prodjctId, octAmount + novAmount)
}
// over50SoldAndAmountRDD.collect.foreach(println)
/*
결과값
(8,140)
(15,131)
*/

//내부결합과 외부 결합
val animalsEn = sc.parallelize(List((0,"cat"),(1, "dog"), (2, "monkey"), (3, "tiger")))
val animalsKo = sc.parallelize(List((0,"고양이"),(1, "개"), (2, "monkey"), (3, "호랑이")))
val animalsEnToKo = animalsEn.leftOuterJoin(animalsKo)

/*
animalsEnToKo.collect.foreach(println)
*** 결과 ***
(0,(cat,Some(고양이)))
(1,(dog,Some(개)))
(2,(monkey,Some(monkey)))
(3,(tiger,Some(호랑이)))
*/

import scala.collection.mutable.HashMap
import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

val productsMap = new HashMap[String, (String, Int)]
val hadoopConf = new Configuration
val fileSystem = FileSystem.get(hadoopConf)
val inputStream = fileSystem.open(new Path("/data/products.csv"))
val productsCSVReader = new BufferedReader(new InputStreamReader(inputStream))
var line = productsCSVReader.readLine
while (line != null) {
	val splitLine = line.split(",")
	val productId = splitLine(0)
	val productName = splitLine(1)
	val unitPrice = splitLine(2).toInt
	productsMap(productId) = (productName, unitPrice)
	line = productsCSVReader.readLine
}

productsCSVReader.close()

val broadcastedMap = sc.broadcast(productsMap)
//브로드캐스트를 할 경우, 이그젝큐터가 사용 가능하게 전역적으로 사용가능한 값이 됩니다.
//복수의 이그젝큐터가 공통으로 참조하는 데이터는 브로드캐스트 변수로 배포해야 합니다.

val resultRDD = over50SoldAndAmountRDD.map {
	case (productId, amount) =>
		val productsMap = broadcastedMap.value
		val (productName, unitPrice) = productsMap(productId)
		(productName, amount, amount * unitPrice)
}
// resultRDD.collect.foreach(println)
resultRDD.saveAsTextFile("/data/oct-nov-over-50-sold")
//파일은 로컬 or hdfs에 저장된다.


```
