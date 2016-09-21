# Spark MLlib Tutorial - Credit Card Fraud

Spark als Big-Data-Toolset ist in aller Munde.

Mit Spark kann man große Datenmengen, die auf viele
Server verteilt sind parallel verarbeiten, und das
geht (fast) so einfach, als würde man auf einer
lokalen Collection arbeiten.
Spark MLlib ermöglicht Machine Learning Algorithmen auf
diese verteilten Collections anzuwenden und so extrem
rechenaufwändige Prozesse zu skalieren.

Hier ein kleines Tutorial für Scala-Entwickler,
die Scala-Collections und funktionen höherer Ordnung kennen,
aber noch nie mit Spark gearbeitet haben.

Das Projekt könnt ihr als Maven-Projekt direkt in Eure Entwicklungsumgebung laden,
in IntelliJ beispielsweise mit *"File | Open"*.

## Der `SparkContext` und ein *RDD*

Der Einstiegspunkt in Spark ist der `SparkContext`.
Wenn Ihr wissen wollt, wie man einen erstellt,
schaut in den Code der Methode `getSparkContext`.
Für unsere Tests arbeitet Spark im lokalen Modus,
d. h. alle benötigten Dienste werden in der selben
JVM gestartet.

Die einfachste verteilte Collection in Spark nennt man *RDD*.
Spark kann Daten aus den verschiedensten Quellen lesen,
z. B. aus hdfs-Dateien in Hadoop, aus lokalen Dateien oder aus
relationalen Datenbanken.
Für Testzwecke kann man auch eine Scala-`List` als Datenquelle
heranziehen.
Die Methode `parallelize()` macht daraus ein *RDD* -> `sc parallelize List(1, 2, 3)`

Für einige Spark MLlib Algorithmen brauchen wir zudem Spark DataFrames, welche
eine tabellarische Form der verteilten Collections sind. Für diesen brauchen wir einen
Spark SQLContext, den uns die Methode `getSparkSQLContext` zur Verfügung stellt.

## Actions

Sogenannte *Actions*, verarbeiten die Daten des RDD *und* liefern
ein Ergebnis an den Client. Ein paar Beispiele:
`count()` zählt die Anzahl Zeilen,
`foreach(..)` führt für jede Zeile einen Befehl aus (lokal im Client),
`reduce(..)` aggregiert die Daten, nach einer gegebenen Funktion.

```scala
  it should "count RDDs" in {
    sampleRDD.count shouldBe 4
  }

  it should "run foreach action" in {
    sampleRDD foreach (i => println(s"Result contains $i"))
  }

  it should "run reduce action" in {
    sampleRDD reduce (_ + _) shouldBe 42
  }
```
## Wohin mit den Ergebnissen?

Wenn die Ergebnismenge nicht zu gross ist, kann man
sie sich als lokale Scala-`List` mittels `collect()` geben lassen.
Bei grösseren Datenmengen kann man sich mit `takeSample(..)` eine
Stichprobe geben lassen, oder das Ergebnis
mit `saveAsTextFile()` in Dateien schreiben lassen.

**Achtung:** In der Regel schreibt spark die Ergebnisse parallel
in mehrere Dateien. Im Beispiel haben wir die Parallelität
durch `coalesce(1)` reduziert, um nur eine Datei zu bekommen.

```scala
  it should "copy the results of a RDD to a collection" in {
    sampleRDD.collect should contain theSameElementsAs List(3, 10, 20, 9)
  }

  it should "write to file" in {
    val targetDir = "tmp/zahlen"
    deleteDirectory(new File(targetDir))
    sampleRDD coalesce 1 saveAsTextFile targetDir
    (Source fromFile s"$targetDir/part-00000").getLines.toList should have size (4)
  }
```
## Dateien lesen

Mit `textFile(..)` können wir Dateien lesen.

Für unser Beispiel lesen wir Transaktionsdaten von einer fiktiven Bank.
Jede Transaktion ist in einer Zeile abgebildet und die einzelnen Spalten mit | (Pipe) getrennt.

`10000|77848|Online|16.03.2016|13:56|13,30|0`

Die Dateien mit diesen Daten wollen wir einlesen und für das Modelltraining aufbereiten.

```scala
  val dataSource = getClass getResource "/transactions"
  val transactions = sc textFile dataSource.getPath
```
## Persistierung von Zwischenergebnissen

Führt man zwei Aktionen auf demselben *RDD* aus,
dann liest Spark bei der Zweiten Aktion die Datenquelle erneut!
Mit `cache()` kann man Spark anweisen, sich das Zwischenergebnis zu
merken.

```scala
  val data = prepareData(SparkUtils.getSparkContext, SparkUtils.getSparkSQLContext)
  val Array(training, test) = data randomSplit (Array(0.7, 0.3))
  training.cache
  test.cache
  val modelLogisticRegression = trainLogisticRegression(training)
  val predictionLogisticRegression = testLogisticRegression(modelLogisticRegression, test)
  val modelLinearRegression = trainLinearRegression(training)
  val predictionLinearRegression = testLinearRegression(modelLinearRegression, test)
```

## Transformationen

Zur Verarbeitung von RDD's kennt Spark neben *Aktionen*
auch *Transformationen*.

Transformationen werden in den Spark-Workern ausgeführt,
und anders als *Aktionen* übertragen sie kein Ergebnis
zum Client, sondern liefern wieder einen RDD.
Auf diesen kann man, wenn man möchte weitere Transformationen ausführen.

Eine sehr häufig verwendete Transformation ist `map(...)`:

```scala
  val dataSource = getClass getResource "/transactions"
  val transactions = sc textFile dataSource.getPath map {
    r =>
      val dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm")
      val a = r split "\\|"
      val date = new Date(dateFormat parse s"${a(3)} ${a(4)}" getTime)
      Transaction(
        a(0),
        a(1),
        a(2),
        date,
        a(5).replace(",", ".").toFloat,
        a(6) == "1"
      )
  }

```

## Machine Learning

Die von Spark MLlib bereitgestellten Classificator Algorithmen bestehen stets aus zwei Bestandteilen:
1. Estimator - der Funktion, welche zum Training eines Modells (Transformator) aus den bereitgestellten Features und Labels verwendet wird.
```scala
  def trainLogisticRegression(training: DataFrame) = {
    val lr = new LogisticRegression
    lr.setThreshold(0.023)
    lr fit training
  }
```
2. Transformator - das trainierte Modell, welches aus einem neuen Datensatz mit den gleichen Features wie die trainierten Einträge eine Prognose bzgl. des Labels vornimmt.
```scala
  def testLogisticRegression(model: LogisticRegressionModel, test: DataFrame) =
    model transform test select ("prediction", "label") map (r => r(0).toString.toDouble -> r(1).toString.toDouble)
```

![Data Mining Process](https://github.com/cthomsen/sparkmllib-credit-card-fraud/blob/master/dm-process.png)

Um die Qualität eines Modells mit einem Datensatz zu testen teilt man diesen üblicherweise in einen Trainingsdatensatz und einen Testdatensatz.

```scala
  val data = prepareData(SparkUtils.getSparkContext, SparkUtils.getSparkSQLContext)
  val Array(training, test) = data randomSplit (Array(0.7, 0.3))
```

## Aufgabe - Vorhersage von Kreditkartenbetrug

Gegeben sind etwa 150.000 Buchungsdatensätze von denen etwa 1.500 als Betrugsfälle markiert sind.
Mit einem Trainingsset von 70% dieser Daten soll ein Modell trainiert werden, welches mit den verbliebenen 30% getestet wird.
Ziel ist es im Testdatensatz vorherzusagen, um es sich um einen Betrugsfall handelt.
Dabei sind falsche Vorhersagen mit Kosten verbunden. 
False Positives kosten 2% der Überweisungshöhe (die Buchungsgebühr der Bank).
False Negatives kosten 100% der Überweisungshöhe (die Bank erstattet den Schaden im Betrugsfall an den Kunden).
Das beste Modell ist jenes, welches für den Testdatensatz die geringsten Kosten enthält.
