"아파치 스파크 입문, 따라 하며 쉽게 익히는 스파크 SQL, 스트림처리, 머신러닝-사루타 고스케, 도바시 마사루, 요시다 고요, 사사키 도루, 쓰즈키 마사요시"

5장의 복잡한 처리를 효율적으로 하기위한 영속화 예제를 따라해봤습니다.

중요하게 봐야 할 부분은 cache 입니다. cache를 통해 영속화가 이뤄집니다. (영속화는 같은 데이터를 두번 부르지 않기 위해서 cache하는 것입니다.)

간단한 설명은 코드 중간중간에 설명 되어 있습니다.

#### questionnaire.csv
```{text}
23,F,3
22,F,5
20,M,4
35,F,2
33,F,4
18,M,4
28,M,5
42,M,3
18,M,3
56,F,2
53,M,1
30,F,4
19,F,5
17,F,4
33,M,4
26,F,3
22,F,2
27,M,4
45,F,2
```

#### code
```{.scala}
var questionnaireRDD = sc.textFile("/data/questionnaire.csv").map {
	record => 
		val splitRecord = record.split(",")
		val ageRange = splitRecord(0).toInt
		val maleOrFemale = splitRecord(1)
		val point = splitRecord(2).toInt
		(ageRange, maleOrFemale, point)
}

//RDD 영속화
questionnaireRDD.cache

val numQuestionnaire = questionnaireRDD.count

//RDD 요소의 개수 확인과 집계
val totalPoints = questionnaireRDD.map(_._3).sum
/*
println(s"AVG ALL: ${totalPoints / numQuestionnaire}")
**결과**
totalPoints: Double = 64.0
AVG ALL: 3.3684210526315788
*/

//평균계산하기
val (totalPoints, numQuestionnaire) = 
	questionnaireRDD.map ( record => (record._3, 1)).reduce {
		case ((intermedPoint, intermedCount), (point, one)) =>
			(intermedPoint + point, intermedCount + one)
	}
/*
println(s"AVG ALL: ${totalPoints / numQuestionnaire}")
**결과**
numQuestionnaire: Long = 19
totalPoints: Double = 64.0
totalPoints: Int = 64
numQuestionnaire: Int = 19
AVG ALL: 3
*/

// 연령별 평균 계산하는 방법
val agePointOneRDD = questionnaireRDD.map(record => (record._1, (record._3, 1)))

val totalPtAndCntPerAgeRDD = agePointOneRDD.reduceByKey {
	case ((intermedPoint, intermedCount), (point, one)) =>
		(intermedPoint + point, intermedCount + one)
}

totalPtAndCntPerAgeRDD.collect.foreach {
	case (ageRange, (totalPoint, count)) =>
		println(s"AVG Age Range($ageRange): ${totalPoint / count.toDouble}")
}
/*
**결과**
AVG Age Range(56): 2.0
AVG Age Range(22): 3.5
AVG Age Range(28): 5.0
AVG Age Range(30): 4.0
AVG Age Range(42): 3.0
AVG Age Range(18): 3.5
AVG Age Range(20): 4.0
AVG Age Range(26): 3.0
AVG Age Range(19): 5.0
AVG Age Range(53): 1.0
AVG Age Range(35): 2.0
AVG Age Range(27): 4.0
AVG Age Range(33): 4.0
AVG Age Range(23): 3.0
AVG Age Range(45): 2.0
AVG Age Range(17): 4.0
*/
```
