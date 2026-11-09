/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.LanceVectorSearch;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * Lance vector search UDF.
 * 
 * <p>Implements TableFunction, supports executing vector search in SQL.
 * 
 * <p>Usage example:
 * <pre>{@code
 * -- Register UDF
 * CREATE TEMPORARY FUNCTION vector_search AS 
 *     'org.apache.flink.connector.lance.table.LanceVectorSearchFunction'
 *     LANGUAGE JAVA USING JAR '/path/to/flink-connector-lance.jar';
 * 
 * -- Use UDF for vector search
 * SELECT * FROM TABLE(
 *     vector_search('/path/to/dataset', 'embedding', ARRAY[0.1, 0.2, 0.3], 10, 'L2')
 * );
 * }</pre>
 */
@FunctionHint(
    output = @DataTypeHint("ROW<id BIGINT, content STRING, embedding ARRAY<FLOAT>, _distance DOUBLE>")
)
public class LanceVectorSearchFunction extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceVectorSearchFunction.class);

    private transient LanceVectorSearch vectorSearch;
    private String currentDatasetPath;
    private String currentColumnName;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.info("Opening LanceVectorSearchFunction");
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing LanceVectorSearchFunction");
        
        if (vectorSearch != null) {
            try {
                vectorSearch.close();
            } catch (Exception e) {
                LOG.warn("Failed to close vector searcher", e);
            }
            vectorSearch = null;
        }
        
        super.close();
    }

    /**
     * Execute vector search
     *
     * @param datasetPath Dataset path
     * @param columnName Vector column name
     * @param queryVector Query vector
     * @param k Number of nearest neighbors to return
     * @param metric Distance metric type: L2, Cosine, Dot
     */
    public void eval(String datasetPath, String columnName, Float[] queryVector, Integer k, String metric) {
        try {
            // Check if need to reinitialize vector searcher
            if (vectorSearch == null || 
                !datasetPath.equals(currentDatasetPath) || 
                !columnName.equals(currentColumnName)) {
                
                if (vectorSearch != null) {
                    vectorSearch.close();
                }
                
                LanceOptions.MetricType metricType = LanceOptions.MetricType.fromValue(
                        metric != null ? metric : "L2"
                );
                
                // The table-function signature only exposes the metric; the remaining search
                // knobs (vector.nprobes / vector.ef / vector.refine-factor) are not reachable
                // from SQL, so the LanceVectorSearch defaults apply here. Build the searcher
                // directly with LanceVectorSearch.builder() when those must be tuned.
                vectorSearch = LanceVectorSearch.builder()
                        .datasetPath(datasetPath)
                        .columnName(columnName)
                        .metricType(metricType)
                        .build();
                
                vectorSearch.open();
                
                currentDatasetPath = datasetPath;
                currentColumnName = columnName;
            }
            
            // Convert query vector
            float[] query = new float[queryVector.length];
            for (int i = 0; i < queryVector.length; i++) {
                query[i] = queryVector[i] != null ? queryVector[i] : 0.0f;
            }
            
            // Execute search
            int topK = k != null ? k : 10;
            List<LanceVectorSearch.SearchResult> results = vectorSearch.search(query, topK);
            
            // Output results
            for (LanceVectorSearch.SearchResult result : results) {
                RowData rowData = result.getRowData();
                double distance = result.getDistance();
                
                // Build output Row
                Row outputRow = convertToRow(rowData, distance);
                if (outputRow != null) {
                    collect(outputRow);
                }
            }
            
        } catch (Exception e) {
            LOG.error("Vector search failed", e);
            throw new RuntimeException("Vector search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Simplified vector search (using default parameters)
     *
     * @param datasetPath Dataset path
     * @param columnName Vector column name
     * @param queryVector Query vector
     * @param k Number of nearest neighbors to return
     */
    public void eval(String datasetPath, String columnName, Float[] queryVector, Integer k) {
        eval(datasetPath, columnName, queryVector, k, "L2");
    }

    /**
     * Most simplified vector search
     *
     * @param datasetPath Dataset path
     * @param columnName Vector column name
     * @param queryVector Query vector
     */
    public void eval(String datasetPath, String columnName, Float[] queryVector) {
        eval(datasetPath, columnName, queryVector, 10, "L2");
    }

    // ==================== BigDecimal[] parameter overloads ====================
    // ARRAY[0.1, 0.2, ...] in Flink SQL is parsed as BigDecimal[] type

    /**
     * Execute vector search (supports BigDecimal[] parameter)
     * 
     * <p>ARRAY[0.1, 0.2, ...] literals in Flink SQL are parsed as DECIMAL type arrays,
     * so this method overload is needed for support.
     *
     * @param datasetPath Dataset path
     * @param columnName Vector column name
     * @param queryVector Query vector (BigDecimal array)
     * @param k Number of nearest neighbors to return
     * @param metric Distance metric type: L2, Cosine, Dot
     */
    public void eval(String datasetPath, String columnName, BigDecimal[] queryVector, Integer k, String metric) {
        Float[] floatVector = convertBigDecimalToFloat(queryVector);
        eval(datasetPath, columnName, floatVector, k, metric);
    }

    /**
     * Simplified vector search (BigDecimal[] parameter)
     */
    public void eval(String datasetPath, String columnName, BigDecimal[] queryVector, Integer k) {
        eval(datasetPath, columnName, queryVector, k, "L2");
    }

    /**
     * Most simplified vector search (BigDecimal[] parameter)
     */
    public void eval(String datasetPath, String columnName, BigDecimal[] queryVector) {
        eval(datasetPath, columnName, queryVector, 10, "L2");
    }

    // ==================== Double[] parameter overloads ====================
    // In some cases parameters may be parsed as Double[] type

    /**
     * Execute vector search (supports Double[] parameter)
     */
    public void eval(String datasetPath, String columnName, Double[] queryVector, Integer k, String metric) {
        Float[] floatVector = convertDoubleToFloat(queryVector);
        eval(datasetPath, columnName, floatVector, k, metric);
    }

    /**
     * Simplified vector search (Double[] parameter)
     */
    public void eval(String datasetPath, String columnName, Double[] queryVector, Integer k) {
        eval(datasetPath, columnName, queryVector, k, "L2");
    }

    /**
     * Most simplified vector search (Double[] parameter)
     */
    public void eval(String datasetPath, String columnName, Double[] queryVector) {
        eval(datasetPath, columnName, queryVector, 10, "L2");
    }

    // ==================== float[] primitive array parameter overloads ====================

    /**
     * Execute vector search (supports float[] primitive array parameter)
     */
    public void eval(String datasetPath, String columnName, float[] queryVector, Integer k, String metric) {
        Float[] floatVector = new Float[queryVector.length];
        for (int i = 0; i < queryVector.length; i++) {
            floatVector[i] = queryVector[i];
        }
        eval(datasetPath, columnName, floatVector, k, metric);
    }

    /**
     * Convert BigDecimal array to Float array
     */
    private Float[] convertBigDecimalToFloat(BigDecimal[] decimals) {
        if (decimals == null) {
            return new Float[0];
        }
        Float[] result = new Float[decimals.length];
        for (int i = 0; i < decimals.length; i++) {
            result[i] = decimals[i] != null ? decimals[i].floatValue() : 0.0f;
        }
        return result;
    }

    /**
     * Convert Double array to Float array
     */
    private Float[] convertDoubleToFloat(Double[] doubles) {
        if (doubles == null) {
            return new Float[0];
        }
        Float[] result = new Float[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            result[i] = doubles[i] != null ? doubles[i].floatValue() : 0.0f;
        }
        return result;
    }

    /**
     * Convert RowData to Row
     */
    private Row convertToRow(RowData rowData, double distance) {
        if (rowData == null) {
            return null;
        }
        
        if (rowData instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) rowData;
            int arity = genericRowData.getArity();
            
            // Create new Row including distance field
            Object[] values = new Object[arity + 1];
            for (int i = 0; i < arity; i++) {
                Object field = genericRowData.getField(i);
                values[i] = convertField(field);
            }
            values[arity] = distance;
            
            return Row.of(values);
        }
        
        return null;
    }

    /**
     * Convert field value
     */
    private Object convertField(Object field) {
        if (field == null) {
            return null;
        }
        
        if (field instanceof StringData) {
            return ((StringData) field).toString();
        }
        
        if (field instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) field;
            int size = arrayData.size();
            Float[] result = new Float[size];
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    result[i] = null;
                } else {
                    result[i] = arrayData.getFloat(i);
                }
            }
            return result;
        }
        
        return field;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("content", DataTypes.STRING()),
                                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
                                DataTypes.FIELD("_distance", DataTypes.DOUBLE())
                        )
                ))
                .build();
    }
}
