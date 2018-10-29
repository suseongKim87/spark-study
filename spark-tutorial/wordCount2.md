## WordCount2 예제

이 예제에서는 더 간단한 방법을 사용합니다. 또한 코드를보다 쉽게 구성 할 수있는 한 가지 방법을 보여주며 입력 및 출력 위치에 대한 변수를 정의합니다.

우리는 지금 `SparkSession`을 생성하고 필요할 때 그것으로부터`SparkContext`를 추출을 권장하는 접근법을 사용할 것입니다. 마지막으로 (http://spark.apache.org/docs/latest/tuning.html)을 사용하여 더 나은 압축을 제공하므로 메모리와 네트워크 대역을 더 잘 활용할 수 있습니다.

데이터 구조는 아래와 같습니다.
```{.text}
책|장|절|내용
```

 장과 절, 그리고 내용으로 구분 된 필드들입니다. 책 이름을 포함하면 결과가 크게 바뀌지 않지만 구절의 단어 수를 계산하기만 하면됩니다. 

```{.scala}
val in  = "../data/kjvdat.txt"    // input
val out = "output/kjv-wc3"        // output

def toText(str: String): String = {
  val ary = str.toLowerCase.split("\\s*\\|\\s*")
  if (ary.length > 0) ary.last else ""
}
```
위 코드에서 우리는 입력파일과 출력파일의 위치를 받았고, String 값을 받은 후, 소문자로 만들고, '|'로 잘라낸 후, ary의 마지막 데이터를 반환하는 method를 구현했습니다. 

```{.scala}
val input = sc.textFile(in).map(line => toText(line)) 

input.cache
```
읽어 드린 파일을 toText합수를 통해 잘라내고 Array[String]형태롤 반환합니다. 또한, `input`을 여러 번 읽으려고 할 때 매번 디스크에서 다시 읽지 않도록 데이터를 캐시합니다. 

```{.scala}
val wc1 = input
  .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
  .countByValue()

(wc1 => Map(onam -> 4, professed -> 2, confesseth -> 3, brink -> 6, youthful -> 1, healings -> 1, sneezed -> 1, forgotten -> 46, precious -> 76, inkhorn -> 3, exorcists -> 1, derided -> 2, eatest -> 3, lover -> 4, centurion -> 21, plentiful -> 4, pasture -> 20, sargon -> 1, speaker -> 2...)
```
 flatMap은 단어의 RDD 이후로 반환 된 각 배열을 flatten하게 합니다. 이후, 
 `countByValue`를 사용하여 각 단어를 키로 처리 한 다음 모든 키를 계산합니다. 이것은 스칼라 맵 [String, Long]을 드라이버에 반환하므로 매우 큰 데이터 세트의 OutOfMemory (OOM) 오류에주의 해야 합니다. 이작업이 끝난 후에는 더 이상 RDD의 값이 아니라 scala Map[T, Long] 드라이버 값을 반환 합니다.


```{.scala}
val wc2 = wc1.map(key_value => s"${key_value._1},${key_value._2}").toSeq  // returns a Seq[String]
val wc = sc.makeRDD(wc2, 1)

println(s"Writing output to: $out")
wc.saveAsTextFile(out)
```
출력을 위해 RDD의 값으로 변환하는 작업을 합니다. 먼저 쉽표로 구분된 문자열로 변환합니다. wc1의 값을 받아 데이터를 정의한 후, 데이터를 `makeRDD`를 사용해, RDD 형태로 만든 후, output  값에 데이터를 저장 합니다.