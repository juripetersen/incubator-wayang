package org.apache.wayang.basic.operators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.fs.FileSystems;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.IcebergGenerics.ScanBuilder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.commons.lang3.Validate;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;

public class ApacheIcebergSource extends UnarySource<Record> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private Catalog catalog;
    private String icebergTableName;
    private String icebergTableFolderBase;

    private List<Expression> whereExpressions;
    private Collection<String> columns;

    private org.apache.iceberg.Table cachedTable = null;

    private static final Double defaultSelectivityValue = 0.10;

    public static ApacheIcebergSource create(Catalog catalog, String icebergTableName, String IcebergTableFolderBase,
            List<Expression> whereExpressions, Collection<String> columns) {
        ApacheIcebergSource ApacheIcebergSource = new ApacheIcebergSource(catalog, icebergTableName,
                IcebergTableFolderBase, whereExpressions, columns);
        return ApacheIcebergSource;
    }

    private ApacheIcebergSource(Catalog catalog, String icebergTableName, String IcebergTableFolderBase,
        List<Expression> whereExpressions, Collection<String> columns) {
        super(createOutputDataSetType(columns));
        this.catalog = catalog;
        this.icebergTableName = icebergTableName;
        this.icebergTableFolderBase = IcebergTableFolderBase;

        this.whereExpressions = whereExpressions;
        this.columns = columns;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ApacheIcebergSource(ApacheIcebergSource that) {
        super(that);
        this.catalog = that.getCatalog();
        this.columns = that.getColumns();
        this.icebergTableName = that.getIcebergTableName();
        this.icebergTableFolderBase = that.getIcebergTableFolderBase();
        this.whereExpressions = that.getWhereExpressions();
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new ApacheIcebergSource.CardinalityEstimator());
    }

    /**
     * Custom
     * {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for
     * {@link FlatMapOperator}s.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext,
                CardinalityEstimate... inputEstimates) {
            Validate.isTrue(ApacheIcebergSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities");

            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(),
                    ApacheIcebergSource.this.icebergTableName); // ApacheIcebergSource.this.inputUrl);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey,
                    CardinalityEstimate.class);
            if (cardinalityEstimate != null)
                return cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
            OptionalLong fileSize = getFileSize();
            if (fileSize.isEmpty()) {
                ApacheIcebergSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        ApacheIcebergSource.this.icebergTableName);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }
            if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalLong numberRows = ApacheIcebergSource.this.ExtractNumberOfRows();

            if (numberRows.isEmpty()) {
                ApacheIcebergSource.this.logger
                        .warn("Could not determine the cardinality of {}... deliver fallback estimate.", ApacheIcebergSource.this.icebergTableName);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            long rowCount = numberRows.getAsLong();
            cardinalityEstimate = new CardinalityEstimate(rowCount, rowCount, 1d);

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }
    }

    private static DataSetType<Record> createOutputDataSetType(Collection<String> columnNames) {
        String[] columnNamesAsArray = (String[]) columnNames.toArray();
        return columnNamesAsArray.length == 0 ? DataSetType.createDefault(Record.class)
                : DataSetType.createDefault(new RecordType(columnNamesAsArray));
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public String getIcebergTableName() {
        return icebergTableName;
    }

    public String getIcebergTableFolderBase() {
        return icebergTableFolderBase;
    }

    public List<Expression> getWhereExpressions() {
        return whereExpressions;
    }

    public Collection<String> getColumns() {
        return columns;
    }

    private void setCachedTable(Table table) {
        this.cachedTable = table;
    }

    // private void setCachedScanBuilder(ScanBuilder scanBuilder) {
    //     this.cachedScanBuilder = scanBuilder;
    // }

    private org.apache.iceberg.Table getTable() {
        if (this.cachedTable != null) {
            return this.cachedTable;
        }

        TableIdentifier tableId = TableIdentifier.of(getIcebergTableFolderBase(), getIcebergTableName());
        Table table = this.catalog.loadTable(tableId);
        setCachedTable(table);
        return table;
    }

    // private ScanBuilder getScanBuilder() {
    //     if (this.cachedScanBuilder != null) {
    //         return cachedScanBuilder;
    //     }
    //     ScanBuilder scanBuilder = IcebergGenerics.read(getTable());
    //     if (this.whereExpressions.size() > 0) {
    //         for (Expression whereExpression : this.whereExpressions) {
    //             scanBuilder = scanBuilder.where(whereExpression);
    //         }
    //     }
    //     setCachedScanBuilder(scanBuilder);
    //     return scanBuilder;
    // }

    // does not account if a filter is needed
    private OptionalLong ExtractNumberOfRows() {
        try {
            long rowCount = 0L;

            Table table = getTable();

            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask fileScanTask : tasks) {
                    rowCount += fileScanTask.estimatedRowsCount();
                }
            }

            if (rowCount == 0) {
                return OptionalLong.empty();
            }

            //if there are any filter conditions applied we use the formula nr of rows = total nr of rows * (1 / number of distinct values) -> where distinct values is
            //in this case calculated by a default number as we do not have acccess to distinct values without reading the entire table  
            if (this.whereExpressions != null && this.whereExpressions.size() > 1) {
                //the selectivity is set to 0,10 for each where expression. If there is 1 the seelctivty will be 0,10 if there is 20 it will be 0,20 and etc.
                //Ask zoi about this!

                Double updatedRowCount = rowCount * defaultSelectivityValue;
                return OptionalLong.of(updatedRowCount.longValue());
            }

            return OptionalLong.of(rowCount);

        } catch (Exception e) {
            this.logger.warn("Could not extract the number of rows. Returning empty. Got erro:  " + e);
            return OptionalLong.empty();
        }

    }

    private OptionalLong getFileSize() {

        try {
            long fileSizeCount = 0L;
            try (CloseableIterable<FileScanTask> tasks = getTable().newScan().planFiles()) {
                for (FileScanTask t : tasks) {
                    fileSizeCount += t.file().fileSizeInBytes();
                }
            }
            return OptionalLong.of(fileSizeCount);

        } catch (Exception e) {
            this.logger.warn("Could not get file size. Returning empty. Got error:  " + e);
            return OptionalLong.empty();

        }

    }


}
