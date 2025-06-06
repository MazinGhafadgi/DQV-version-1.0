package dqv.vonneumann.dataqulaity.app

import dqv.vonneumann.dataqulaity.config.{ConfigurationContextFactory, DQJobConfig, YAMConfigLoader}
import dqv.vonneumann.dataqulaity.enums.SourceType
import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
case class YAMLSchemaConfig(sourceType: String = "", path: String = "")

object YAMLSchemaApp extends App {
  implicit val sparkSession: SparkSession = SparkSessionFactory.createSparkSession("local")

  def apply(args: Array[String]): String = {

   val json =  YAMConfigLoader.toJson("local", DQJobConfig())
    val config = ConfigurationContextFactory.toConfigContexts(json.right.get.toString()).right.get.head

/*
    val parser = new scopt.OptionParser[YAMLSchemaConfig]("JobConfig") {
      opt[String]('s', "source").required().valueName("value is required").action((x, c) => c.copy(sourceType = x)).text("source is required")
      opt[String]('p', "path").required().valueName("value is required").action((x, c) => c.copy(path = x)).text("path is required")
    }

    val yamlSchemaConfig = parser.parse(args, YAMLSchemaConfig()).getOrElse(throw new RuntimeException("JobArgs must be initialised"))
*/

      config.sourceType match {
      case SourceType.CSV => sparkSession.read.option("header", "true").option("inferSchema", "true").csv(config.sourcePath).schema.map(x => x.name + ":" + x.dataType.typeName.toUpperCase).mkString(",")
      case SourceType.Parquet => sparkSession.read.parquet(config.sourcePath).schema.map(x => x.name + ":" + x.dataType.typeName.toUpperCase).mkString(",")
    }
  }

  val dfColumns = apply(args)
  val schemaRef = "\"schema\" : \"http://vonneumann.com/schemas/myschema.json#\""
  val ref = "$ref"
  val bigQueryTableRegExp = "\"[a-zA-Z0–9-_.]+\\\\.[a-zA-Z0–9-]+\\\\.[a-zA-Z0–9-]\""

  val columns = s"""$dfColumns"""

  val doubleQColumns = "[" + columns.replaceAll("([\\w:]+)", "\"$1\"") + "]"
  val word = "\\b(?:INTEGER|LONG|DOUBLE)\\b"

  val inputString =
    s"""
          |{
          |  "$schemaRef,
          |  "type": "array",
          |  "uniqueItems": true,
          |  "items": {
          |    "$ref": "#/definitions/Welcome5Element"
          |  },
          |  "definitions": {
          |    "Welcome5Element": {
          |      "type": "object",
          |      "additionalProperties": false,
          |      "properties": {
          |        "process.type": {
          |          "type": "string",
          |          "enum": ["Batch", "Streaming"]
          |        },
          |        "sink.type": {
          |          "type": "string",
          |          "enum": ["Console", "BigQuery"]
          |        },
          |        "quality.check.type": {
          |          "type": "string",
          |          "enum": ["Reconcile", "QualityCheck"]
          |        },
          |        "source": {
          |          "$ref": "#/definitions/Source"
          |        },
          |        "target": {
          |          "$ref": "#/definitions/Target"
          |        },
          |        "rules": {
          |          "type": "array",
          |          "uniqueItems": true,
          |          "items": {
          |            "$ref": "#/definitions/RuleElement"
          |          }
          |        }
          |      },
          |      "allOf": [
          |        {
          |          "if": {
          |            "properties": {
          |              "quality.check.type": {
          |                "const": "Reconcile"
          |              }
          |            }
          |          },
          |          "then" : {
          |          "required": [
          |            "process.type",
          |            "rules",
          |            "sink.type",
          |            "source",
          |            "target"
          |          ]
          |          },
          |          "else": {
          |            "required": [
          |              "process.type",
          |              "rules",
          |              "sink.type",
          |              "quality.check.type",
          |              "source"
          |            ]
          |          }
          |        }],
          |      "title": "Welcome5Element"
          |    },
          |    "RuleElement": {
          |      "type": "object",
          |      "additionalProperties": false,
          |      "properties": {
          |        "rule": {
          |          "$ref": "#/definitions/RuleRule"
          |        }
          |      },
          |      "required": [
          |        "rule"
          |      ],
          |      "title": "RuleElement"
          |    },
          |    "RuleRule": {
          |      "type": "object",
          |      "additionalProperties": false,
          |      "properties": {
          |        "type": {
          |          "type": "string",
          |          "enum": ["NullCheck", "InRangeCheck", "NonNegativeCheck", "UniquenessCheck", "PositiveCheck", "EmailCheck", "MSISDNCheck", "ReconcileRule"]
          |        },
          |        "column": {
          |          "type": "string",
          |          "enum": $doubleQColumns
          |        },
          |        "description": {
          |          "type": "string"
          |        }
          |      },
          |      "if": {
          |        "properties": { "type": { "enum": ["NonNegativeCheck", "PositiveCheck"] } }
          |      },
          |      "then": {
          |        "properties": { "column": { "pattern": "$word" } }
          |      },
          |      "else": {
          |        "properties": { "column": { "pattern": "^." } }
          |      },
          |      "required": [
          |        "description",
          |        "type",
          |        "column"
          |      ],
          |      "title": "RuleRule"
          |    },
          |    "Source": {
          |      "type": "object",
          |      "additionalProperties": false,
          |      "properties": {
          |        "source.type": {
          |          "type": "string",
          |          "default" : "CSV",
          |          "enum": ["CSV", "Parquet", "BigQuery"]
          |        },
          |        "source.path": {
          |          "type": "string",
          |          "default": "projectId.dataset.tableName"
          |        }
          |      },
          |      "if": {
          |        "properties": { "source.type": { "enum": ["CSV", "Parquet"] } }
          |      },
          |      "then": {
          |        "properties": { "source.path": { "pattern": "^gs://" } }
          |      },
          |      "else": {
          |        "properties": { "source.path": { "pattern": $bigQueryTableRegExp } }
          |      },
          |      "required": [
          |        "source.path",
          |        "source.type"
          |      ],
          |      "title": "Source"
          |    },
          |    "Target": {
          |      "type": "object",
          |      "additionalProperties": false,
          |      "properties": {
          |        "target.type": {
          |          "type": "string",
          |          "default" : "CSV",
          |          "enum": ["CSV", "Parquet", "BigQuery"]
          |        },
          |        "target.path": {
          |          "type": "string",
          |          "default": "projectId.dataset.tableName"
          |        }
          |      },
          |      "if": {
          |        "properties": { "target.type": { "enum": ["CSV", "Parquet"] } }
          |      },
          |      "then": {
          |        "properties": { "target.path": { "pattern": "^gs://" } }
          |      },
          |      "else": {
          |        "properties": { "target.path": { "pattern": $bigQueryTableRegExp } }
          |      },
          |      "required": [
          |        "target.path",
          |        "target.type"
          |      ],
          |      "title": "Target"
          |    }
          |  }
          |}
        """.stripMargin

  val writer = new PrintWriter(new File("src/main/resources/schema.json"))
  writer.write(inputString.mkString)
  writer.close()

}