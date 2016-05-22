# parsebox
Small parsing library for Apache Spark

[![Build Status](https://travis-ci.org/sadikovi/parsebox.svg?branch=master)](https://travis-ci.org/sadikovi/parsebox)
[![codecov](https://codecov.io/gh/sadikovi/parsebox/branch/master/graph/badge.svg)](https://codecov.io/gh/sadikovi/parsebox)

Parsebox provides Scala/Java compatible interfaces to implement file-based and
Spark-package-based parsers similar to Spark datasource API.

## Usage
The main entry point to manage all defined formats is `ParseboxContext`.
```scala
import com.github.sadikovi.parsebox.ParseboxContext
val pc = ParseboxContext.getOrCreate(sqlContext)
```

`ParseboxContext` provides methods to load a specific `DefaultFormat` providing, optionally,
options and paths to files. Here is an example of loading JSON format based on Spark JSON
datasource:
```scala
// We use short name for JSON, because it is a registered format
// As per Spark 1.5.0 there is only one file pattern is supported
// for datasource
val df: DataFrame = pc.format("json").load("file:/.../sample.json")
// Example of loading simple CSV format from examples in repository, "DefaultFormat" will be
// automatically added when loading by package
val df: DataFrame = pc.
  format("com.github.sadikovi.parsebox.examples.csv").
  load("file:/.../sample.csv")
// This is the alternative with full class name
val df: DataFrame = pc.
  format("com.github.sadikovi.parsebox.examples.csv.DefaultFormat").
  load("file:/.../sample.csv")
```
Adding optional parameters:
```scala
// Loading custom format with optional parameters that does not
// require file paths
val df: DataFrame = pc.format("com.example.text").
  option("key1", "value1").load()
```

### API
Parsebox provides necessary classes to create user formats and types to process data.

#### Creating new types
Implementation and examples are in `types.scala` file. Each type needs to be a subclass of
`RecordType` and overwrite conversion `toRow` and `dataSchema` in order to map final DataFrame to a
schema. It is also recommended to overwrite default `hashCode` for efficiency. Each type must be
implemented as mutable, provide no-args constructor, and setters, as it can be used as key class in
Hadoop `InputFormat`.

In order to use created custom type, you can add it to Parsebox's `TypeRegistry` code directly with
optional short name, or use `TypeRegistry.register()` to register custom type at runtime.

### Creating new formats
To create custom format to provide parsing logic, you need to subclass one of the API classes from
`formats.scala` with concrete implementation of some of the methods. It is recommended to follow
the convention of creating `DefaultFormat` subclass, this allows to provide only package name to
resolve parser, though you can create format with any other class name and provide fully qualified
class name as a format.

Currently these formats are available:
- `BaseFormat` - low-level and generic format, should be used only when tricky custom processing
logic is required, that cannot be implemented using formats below
- `TypedFormat` - low-level generic, record based format, allows to use one of the
defined `RecordType`s
- `ExternalFormat` - format is built mainly for Spark packages, recommended to use over
`TypedFormat` when there is already package or it is simple to implement parsing directly in Spark
- `HadoopFormat` - format is built for file-based (either compressed or uncompressed) parsers,
allows to implement custom processing of a record, providing custom delimiter (as an opposite to
default new line separator), failure handling and filtering raw records. It is recommended for most
use cases

You can look at examples in [this folder](src/main/scala/com/github/sadikovi/parsebox/examples)
