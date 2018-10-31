```{.scala}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

val conf = new SparkConf()
val sc = new SparkContext(conf)
val sqlContext = new HiveContext(sc)

import sqlContext.implicits._

case class Desssert(menuId: String, name: String, price: Int, kcal: Int)

val dessertRDD = sc.textFile("/data/dessert-menu.csv")

//toDF를 통해 DataFrame으로 변경
val dessertDF = dessertRDD.map { record =>
		val splitRecord = record.split(",")
		val menuId = splitRecord(0)
		val name = splitRecord(1)
		val price = splitRecord(2).toInt
		val kcal = splitRecord(3).toInt
		Desssert(menuId, name, price, kcal)
}.toDF

//dessertDF의 schema를 확인
dessertDF.printSchema

//DataFrame을 RDD로 생성
val rowRDD = dessertDF.rdd
/*
rowRDD.collect.foreach(println)
*** result ***
[D-0,초콜릿 파르페,4900,420]
[D-1,푸딩 파르페,5300,380]
[D-2,딸기 파르페,5200,320]
[D-3,판나코타,4200,283]
[D-4,치즈 무스,5800,288]
[D-5,아포가토,3000,248]
[D-6,티라미스,6000,251]
[D-7,녹차 파르페,4500,380]
[D-8,바닐라 젤라또,3600,131]
[D-9,카라멜 팬케익,3900,440]
[D-10,크림 안미츠,5000,250]
[D-11,고구마 파르페,6500,650]
[D-12,녹차 빙수,3800,320]
[D-13,초코 크레이프,3700,300]
[D-14,바나나 크레이프,3300,220]
[D-15,커스터드 푸딩,2000,120]
[D-16,초코 토르테,3300,220]
[D-17,치즈 수플레,2200,160]
[D-18,호박 타르트,3400,230]
[D-19,캬라멜 롤,3700,230]
[D-20,치즈 케익,4000,230]
[D-21,애플 파이,4400,350]
[D-22,몽블랑,4700,290]
*/

val nameAndPriceRDD = rowRDD.map { row =>
	val name = row.getString(1)
	val price = row.getInt(2)
	(name, price)
}
/*
nameAndPriceRDD.collect.foreach(println)
*** result ***
(초콜릿 파르페,4900)
(푸딩 파르페,5300)
(딸기 파르페,5200)
(판나코타,4200)
(치즈 무스,5800)
(아포가토,3000)
(티라미스,6000)
(녹차 파르페,4500)
(바닐라 젤라또,3600)
(카라멜 팬케익,3900)
(크림 안미츠,5000)
(고구마 파르페,6500)
(녹차 빙수,3800)
(초코 크레이프,3700)
(바나나 크레이프,3300)
(커스터드 푸딩,2000)
(초코 토르테,3300)
(치즈 수플레,2200)
(호박 타르트,3400)
(캬라멜 롤,3700)
(치즈 케익,4000)
(애플 파이,4400)
(몽블랑,4700)
*/

//DataFrame 조작을 위해 query 발생
//DataFrame 에서 직접쿼리x, registerTempTable 을 통해 임시 테이블을 을 만들어 쿼리를 발생.
dessertDF.registerTempTable("dessert_table")

val numOver300KcalDF = sqlContext.sql(
	"SELECT count(*) AS numOver300Kcal FROM dessert_table WHERE kcal >= 260")


```
