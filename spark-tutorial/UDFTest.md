### 스파크 SQL의 udf(user defined function) 사용하기

UDF를 사용할 때, UDFRegistration의 register를 사용합니다. register는 2개의 매개변수를 받는데, 

첫번째는 정의하려는 UDF의 이름, 두번째는 스칼라 함수로 넘겨진 스칼라 함수가 UDF의 본체롤 다뤄집니다.

```{.scala}
val strlen = sqlContext.udf.register("stdlen", (str:String) => str.length)
sqlContext.sql("SELECT stdlen('Hello 스파크 SQL') AS result_of_strlen").show
/*
*** result ***
+----------------+
|result_of_strlen|
+----------------+
|              13|
+----------------+
*/
```

위 내용은 scala의 함수를 지정해서 하는 경우이며, hive UDF를 통해 jar를 배포해서 하는 경우가 일반적이라고 볼 수 있습니다.

아래는 hive UDF를 만드는 방법을 기술해 놓은 것 입니다. 

#### 1. java의 jar를 만들기 위해 maven을 사용해 프로젝트를 생성합니다.

#### 2. spark-core, spark-sql, hive-exec 의 dependency를 pom.xml파일에 등록해 줍니다.
   (저는 spark-core(2.3.0), spark-sql(2.3.0), hive-exec(2.3.0) 버전을 사용했습니다.)

#### 3. package를 만들고 java 파일을 만들어 줍니다. (ex> com.spark.hive.sql.udf.hive_strlen.java)

#### 4. 아래의 코드와 같이 org.apache.hadoop.hive.ql.exec.UDF 를 상속받아 원하는 function을 만들어 줍니다.
```{.java}
package com.spark.hive.sql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

//글자 수 count function
public class hive_strlen extends UDF {

    public Text evaluate(Text input) {
        if(input == null) return null;
        return new Text(new Integer(input.getLength()).toString());
    }
}
```

#### 5. 다 만들어진 프로젝트를 maven install을 통해 jar 파일을 만들어 줍니다.

#### 6. 만들어지 jar 파일을 spark-shell을 시작하면서 추가해 줍니다.
```{.scala}
# .spark-shell --jars hive-udf-test.jar
```

다른 방법으로는 spark-shell 접속 후, [ :require ] 옵션을 사용해 추가하는 방법이 있습니다.

```{.scala}
사용법 => :require <path>          add a jar to the classpath
scala> :require hive-udf-test.jar
```

위와 같이 작성한 후, "CREATE TEMPORARY FUNCTION"을 통해 import한 후, 사용하면 됩니다.
```{.scala}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

val conf = new SparkConf()
val sc = new SparkContext(conf)
val sqlContext = new HiveContext(sc)

//UDF 등록
sqlContext.sql("CREATE TEMPORARY FUNCTION hive_strlen AS 'com.spark.udf.test.JavaWordCount'")

sqlContext.sql("SELECT hive_strlen('Hello Hive UDF') AS result_of_hive_strlen").show
/*
*** result ***
+---------------------+
|result_of_hive_strlen|
+---------------------+
|                   14|
+---------------------+
*/
```
