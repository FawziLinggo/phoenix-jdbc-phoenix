/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.alldata.custom.dialect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;


/**
 * A {@link DatabaseDialect} for Phoenix.
 */

public class PhoenixDatabaseDialect extends GenericDatabaseDialect {


  /**
   * The provider for {@link PhoenixDatabaseDialect}.
   */

  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(PhoenixDatabaseDialect.class.getSimpleName(), "phoenix", "avatica");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new PhoenixDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public PhoenixDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL(36  ,"
                + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          return super.getSqlType(field);
      }
    }

    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INTEGER";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR";
      case BYTES:
        return "VARBINARY";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public List<String> buildAlterTable(
        TableId table,
        Collection<SinkRecordField> fields
  ) {
    // Phoenix does not support ALTER TABLE
    return Collections.emptyList();
  }


  @Override
  public String buildCreateTableStatement(
        TableId table,
        Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    builder.append(")");

    // remove double quotes from the table name
    return builder.toString().replace("\"", "");
  }

  @Override
  public String buildUpsertQueryStatement(
        TableId table,
        Collection<ColumnId> keyColumns,
        Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPSERT INTO ");
    builder.append(table);
    builder.append("(");
    builder.appendList()
          .delimitedBy(",")
          .transformedBy(ExpressionBuilder.columnNames())
          .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    // remove double quotes from the table name
    return builder.toString().replace("\"", "");
  }

}