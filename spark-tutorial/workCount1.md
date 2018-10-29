## 02: WordCount1

이 인기있는 알고리즘을 사용하여 Spark의 [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd)를 사용하여 데이터로 작업하는 관용구를 학습합니다. .RDD) API를 사용합니다.

기본적인 Word Count 알고리즘은 이해하기 쉽고 병렬 계산에 적합하므로 Big Data API를 처음 배울 때 좋은 예제입니다.

다음 예제에서는 commandd-line의 입력변수를 사용하여 다른 입력 소스를 지정할 수있는 기능이 추가됩니다.

코드를 살펴 보겠습니다. 노트북 환경은 이미`sc`라는 이름의`SparkContext` 인스턴스를 만들었습니다. 도입부에서와 마찬가지로 텍스트 파일을 라인별로 읽어 소문자로 변환합니다.

```{.scala}
val input = sc.textFile("../data/kjvdat.txt").map(line => line.toLowerCase)
```

`input`을 여러 번 읽으려고 할 때 매번 디스크에서 다시 읽지 않도록 데이터를 캐시합니다.

```{.scala}
input.cache
```

1. 각 행을 영문 or 숫자가 아닌 문자로 분할하십시오. `flatMap`은 단어의`RDD` 이후로 반환 된 각 배열을 flatten 하게 합니다.
2. 각 작업을 매핑하고 단어와 count가 1 인 튜플을 반환합니다.
3. 각 단어를 키 (첫 번째 튜플 요소)로 사용하고 그 다음 합계를 집계하여 그룹을 "축소"하고`(단어, N) '튜플을 반환하는 SQL`GROUPBY`에 해당하는 작업을 수행합니다.

```{.scala}
val wc = input
  .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))  // RDD[String]
  .map(word => (word, 1))
  .reduceByKey((count1, count2) => count1 + count2)  
```

 가장 많이 사용하는 방법으로 각 함수 인수에`_`를 사용하여 마지막 줄을`reduceByKey (_ + _)`로 변환하는 것입니다.
```{.scala}
.reduceByKey((count1, count2) => count1 + count2)  
=> .reduceByKey((_, _) => _ + _) 
```