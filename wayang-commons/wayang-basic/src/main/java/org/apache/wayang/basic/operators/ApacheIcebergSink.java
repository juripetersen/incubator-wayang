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
package org.apache.wayang.basic.operators;

import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.FileFormat;

/**
 * This {@link UnarySink} writes all incoming data quanta to an iceberg table.
 * Either if the table does not exists it will create new, otherwise append.
 * 
 * @param <T> Data Type if the incoming Data Quanta
 */
public class ApacheIcebergSink<T> extends UnarySink<T> {

    protected final Catalog catalog;
    protected final Schema schema;
    protected final TableIdentifier tableIdentifier;
    protected final Class<T> tClass;

    /**
     * Creates a new sink.
     *
     * @param catalog         Iceberg catalog used to resolve the target table; must
     *                        not be {@code null}
     * @param schema          Iceberg write schema; must be compatible with the
     *                        target table
     * @param tableIdentifier fully qualified identifier of the target table
     * @param type            {@link DataSetType} of the incoming data quanta
     */
    public ApacheIcebergSink(Catalog catalog, Schema schema, TableIdentifier tableIdentifier,
            DataSetType<T> type) {

        super(type);
        this.catalog = catalog;
        this.schema = schema;
        this.tableIdentifier = tableIdentifier;
        this.tClass = type.getDataUnitType().getTypeClass();

    }

    /**
     * Creates a new sink.
     *
     * @param catalog         Iceberg catalog used to resolve the target table; must
     *                        not be {@code null}
     * @param schema          Iceberg write schema; must be compatible with the
     *                        target table
     * @param tableIdentifier fully qualified identifier of the target table
     * @param typeClass       {@link Class} of incoming data quanta
     */
    public ApacheIcebergSink(Catalog catalog, Schema schema, TableIdentifier tableIdentifier,
            Class<T> typeClass) {
        super(DataSetType.createDefault(typeClass));
        this.catalog = catalog;
        this.schema = schema;
        this.tableIdentifier = tableIdentifier;
        this.tClass = typeClass;
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public ApacheIcebergSink(ApacheIcebergSink<T> that) {
        super(that);
        this.catalog = that.catalog;
        this.schema = that.schema;
        this.tableIdentifier = that.tableIdentifier;
        this.tClass = that.tClass;
    }

    /**
     * Returns the {@link FileFormat} used for writing.
     * Subclasses should override this method to specify a concrete format.
     * 
     * @return the file format
     * @throws UnsupportedOperationException if not overridden
     */
    public FileFormat getFileFormat() {
        throw new UnsupportedOperationException("Subclasses must implement getFileFormat().");
    }
}
