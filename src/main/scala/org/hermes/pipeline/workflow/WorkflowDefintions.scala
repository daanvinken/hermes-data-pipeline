package org.hermes.pipeline.workflow

case class WorkFlow(
                     string: String,
                   )

case class Source(`type`: String, location: String, metadata: Map[String, String])

/* idea:
"""
|{
 |  "source": {
 |    "type": "CSV",
 |    "path": "/home/satendra/data/testing-csv",
 |    "meta":{"text_field":"text","date_field": "date","author_field":"author_name" }
 |  },
 |
 |  "validation": [    "COLUMN_VALIDATION",  "FIELD_VALIDATION"  ],
 |
 |  "transformation": [    "SENTIMENT_ANALYSIS"  ],
 |
 |  "schemaValidation": [    "DATA_MODEL_VALIDATION"  ],
 |
 |  "sink": {
 |    "type": "ES",
 |    "meta":{ "index": "data_index","type": "twitter"    }
 |  }
 |}
"""
*/
