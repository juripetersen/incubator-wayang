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

package org.apache.wayang.applications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.wayang.api.DLTrainingDataQuantaBuilder;
import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.Input;
import org.apache.wayang.basic.model.op.Op;
import org.apache.wayang.basic.model.op.Reshape;
import org.apache.wayang.basic.model.op.Slice;
import org.apache.wayang.basic.model.op.nn.Conv2D;
import org.apache.wayang.basic.model.op.nn.ConvLSTM2D;
import org.apache.wayang.basic.model.op.nn.MSELoss;
import org.apache.wayang.basic.model.optimizer.Adam;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.apache.wayang.basic.operators.DLTrainingOperator;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.tensorflow.Tensorflow;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

public class WeatherForecastPipeline {
    static final int GRID_HEIGHT = 17; // # latitude points
    static final int GRID_WIDTH = 29; // # longitude points
    static final int windowSizeIn = 6;
    static final int windowSizeOut = 3;
    static final String dateToSplit = "2016-09-20 00:00:00+00:00";
    static Logger logger = LogManager.getLogger(WeatherForecastPipeline.class);
    static JavaPlanBuilder planBuilder;

    public static List<DataQuantaBuilder<?, Record>> loadData() {
        String dataFolder = "file:///usr/src/app/files/weather_data/";

        String radarFileName = "radar_sample.parquet";
        DataQuantaBuilder<?, Record> radarData = planBuilder.readParquet(dataFolder + radarFileName, null);

        String rainGaugeFileName = "rain_gauge_sample.parquet";
        DataQuantaBuilder<?, Record> rainGaugeData = planBuilder.readParquet(dataFolder + rainGaugeFileName, null);

        String gridPointsFileName = "grid_points.parquet";
        DataQuantaBuilder<?, Record> gridData = planBuilder.readParquet(dataFolder + gridPointsFileName, null);

        return Arrays.asList(radarData, rainGaugeData, gridData);
    }

    private static Double euclideanDistance(Tuple2<Double, Double> p1, Tuple2<Double, Double> p2) {
        return Math.sqrt(Math.pow(p1.getField0() - p2.getField0(), 2) + Math.pow(p1.getField1() - p2.getField1(), 2));
    }

    private static DataQuantaBuilder<?, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>> mapNearestGridPoint(
            DataQuantaBuilder<?, Tuple2<Double, Double>> radarCoords,
            DataQuantaBuilder<?, Tuple2<Double, Double>> gridCoords
    ) {
        return radarCoords
                .cartesian(gridCoords)
                .map(row -> new Tuple2<>(
                        row.getField0(),
                        new Tuple2<>(row.getField1(), euclideanDistance(row.getField0(), row.getField1()))
                ))
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> t1.getField1().getField1() < t2.getField1().getField1() ? t1 : t2
                )
                .map(row -> new Tuple2<>(row.getField0(), row.getField1().getField0()));
    }

    private static Record appendGridToRecord(Tuple2<Record, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>> row) {
        Record record = row.getField0().copy();
        Tuple2<Double, Double> gridCoord = row.getField1().getField1();
        record.addField(gridCoord);
        return record;
    }

    private static DataQuantaBuilder<?, Record> findNearestGridPoint(
            DataQuantaBuilder<?, Record> radarData,
            DataQuantaBuilder<?, Record> gridData
    ) {
        DataQuantaBuilder<?, Tuple2<Double, Double>> uniqueRadarCoords = radarData
                .map(row -> (Tuple2<Double, Double>) row.getField(row.size() - 1))
                .distinct();
        DataQuantaBuilder<?, Tuple2<Double, Double>> gridCoords = gridData
                .map(row -> (Tuple2<Double, Double>) row.getField(row.size() - 1))
                .distinct();
        DataQuantaBuilder<?, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>> uniqueRadarCoordsWithNearestGridPoint = mapNearestGridPoint(uniqueRadarCoords, gridCoords);

        return radarData
                .join(row -> row.getField(row.size() - 1), uniqueRadarCoordsWithNearestGridPoint, Tuple2::getField0)
                .map(WeatherForecastPipeline::appendGridToRecord);
    }

    private static DataQuantaBuilder<?, Record> aggregateRadarData(DataQuantaBuilder<?, Record> radarData) {
        return radarData
                .map(row -> new Tuple2<>(new Tuple2<>(row.getField(0), row.getField(row.size() - 1)), row.getDouble(4)))
                .reduceByKey(Tuple2::getField0, (t1, t2) -> t1.getField1() < t2.getField1() ? t1 : t2)
                .map(row -> new Record(row.getField0().getField0(), row.getField0().getField1(), row.getField1()));
    }

    private static DataQuantaBuilder<?, Record> applyIdwInterpolation(DataQuantaBuilder<?, Record> rainGaugeData, DataQuantaBuilder<?, Record> gridData) {
        DataQuantaBuilder<?, Tuple2<Double, Double>> allPossibleCoords = gridData
                .map(x -> (Tuple2<Double, Double>) x.getField(x.size() - 1))
                .distinct();

        DataQuantaBuilder<?, Tuple2<Object, Double>> meanPrecipitation = rainGaugeData
                .map(x -> new Tuple2<>(x.getField(0), new Tuple2<>(1, x.getDouble(3))))
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), new Tuple2<>(t1.getField1().getField0() + t2.getField1().getField0(), t1.getField1().getField1() + t2.getField1().getField1())))
                .map(x -> new Tuple2<>(x.getField0(), x.getField1().getField1() / x.getField1().getField0()));

        DataQuantaBuilder<?, Object> timestamps = rainGaugeData
                .map(x -> x.getField(0))
                .distinct();

        return timestamps
                .cartesian(allPossibleCoords)
                .join(Tuple2::getField0, meanPrecipitation, Tuple2::getField0)
                .map(x -> {
                    Object timestamp = x.getField0().getField0();
                    Double precipitation = x.getField1().getField1();
                    Tuple2<Double, Double> coordinates = x.getField0().getField1();
                    return new Record(timestamp, coordinates, precipitation);
                });
    }

    public static DataQuantaBuilder<?, Record> joinRadarAndRainGauge(DataQuantaBuilder<?, Record> radarData, DataQuantaBuilder<?, Record> rainGaugeData) {
        return radarData
                .join(
                        x -> new Tuple2<>(x.getField(0), x.getField(1)),
                        rainGaugeData,
                        x -> new Tuple2<>(x.getField(0), x.getField(1))
                )
                .map(x -> {
                    Object timestamp = x.getField0().getField(0);
                    Tuple2<Double, Double> coordinates = (Tuple2<Double, Double>) x.getField0().getField(1);
                    Double maxReflectivity = x.getField0().getDouble(2);
                    Double precipitation = x.getField1().getDouble(2);
                    return new Record(timestamp, coordinates, maxReflectivity, precipitation);
                });
    }

    public static DataQuantaBuilder<?, Record> sortData(DataQuantaBuilder<?, Record> data) {
        return data
                .sort(x -> {
                    String date = x.getString(0);
                    Tuple2<Double, Double> coordinates = (Tuple2<Double, Double>) x.getField(1);
                    return date + coordinates.getField0().toString() + coordinates.getField1().toString();
                });
    }

    public static List<DataQuantaBuilder<?, Record>> trainTestSplit(DataQuantaBuilder<?, Record> data, String dateToSplit) {
        OffsetDateTime datetimeObject = OffsetDateTime.parse(
                dateToSplit,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX")
        );

        DataQuantaBuilder<?, Record> dataTrain = data.filter(
                row -> OffsetDateTime.parse(row.getString(0)).isBefore(datetimeObject)
        );
        DataQuantaBuilder<?, Record> dataTest = data.filter(
                row -> {
                    OffsetDateTime parsedDate = OffsetDateTime.parse(row.getString(0));
                    return parsedDate.equals(datetimeObject) || parsedDate.isAfter(datetimeObject);
                }
        );
        return Arrays.asList(dataTrain, dataTest);
    }

    public static Record normalizeRecord(Tuple2<Record, List<Double>> row) {
        Record record = row.getField0();
        List<Double> params = row.getField1();
        record.setField(2, (record.getDouble(2) - params.get(0)) / (params.get(1) - params.get(0)));
        record.setField(3, (record.getDouble(3) - params.get(2)) / (params.get(3) - params.get(2)));
        return record;
    }

    public static List<DataQuantaBuilder<?, Record>> normalizeData(DataQuantaBuilder<?, Record> dataTrain, DataQuantaBuilder<?, Record> dataTest) {
        DataQuantaBuilder<?, List<Double>> minMaxValues = dataTrain
                .map(row -> List.of(row.getDouble(2), row.getDouble(2), row.getDouble(3), row.getDouble(3)))
                .reduce((a, b) -> List.of(
                        Math.min(a.get(0), b.get(0)), Math.max(a.get(1), b.get(1)),
                        Math.min(a.get(2), b.get(2)), Math.max(a.get(3), b.get(3))
                ));

        DataQuantaBuilder<?, Record> normalizedDataTrain = dataTrain
                .map(x -> new Tuple2<>(1, x))
                .cartesian(minMaxValues.map(x -> new Tuple2<>(1, x)))
                .map(x -> new Tuple2<>(x.getField0().getField1(), x.getField1().getField1()))
                .map(WeatherForecastPipeline::normalizeRecord);


        DataQuantaBuilder<?, Record> normalizedDataTest = dataTest.map(x -> new Tuple2<>(1, x))
                .cartesian(minMaxValues.map(x -> new Tuple2<>(1, x)))
                .map(x -> new Tuple2<>(x.getField0().getField1(), x.getField1().getField1()))
                .map(WeatherForecastPipeline::normalizeRecord);

        return List.of(normalizedDataTrain, normalizedDataTest);
    }

    public static DataQuantaBuilder<?, Tuple2<Integer, Record>> getContiguousBlocks(DataQuantaBuilder<?, Record> data) {
        AtomicInteger blockCounter = new AtomicInteger(0);
        AtomicReference<OffsetDateTime> prevTimestamp = new AtomicReference<>(null);

        return data.map(record -> {
            OffsetDateTime current = OffsetDateTime.parse(record.getString(0));
            OffsetDateTime previous = prevTimestamp.get();

            int blockId = blockCounter.get();

            if (previous != null && Duration.between(previous, current).toHours() > 1) {
                blockId = blockCounter.incrementAndGet();
            }

            prevTimestamp.set(current);

            return new Tuple2<>(blockId, record);
        });
    }

    public static DataQuantaBuilder<?, Tuple2<List<float[][][][]>, List<float[][][][]>>> windowData(
            DataQuantaBuilder<?, Record> data,
            int windowSizeIn,
            int windowSizeOut
    ) {
        return getContiguousBlocks(data)
                .groupByKey(Tuple2::getField0)
                .map(block -> {
                    List<Tuple2<Integer, Record>> records = StreamSupport
                            .stream(block.spliterator(), false)
                            .toList();

                    int numSnapshots = (int) records.stream()
                            .map(t -> t.getField1().getString(0))
                            .distinct()
                            .count();

                    float[][][] reflectivityArray = new float[numSnapshots][GRID_HEIGHT][GRID_WIDTH];
                    float[][][] precipitationArray = new float[numSnapshots][GRID_HEIGHT][GRID_WIDTH];

                    OffsetDateTime currentTs = null;
                    int snapshotIdx = -1;
                    int gridCellCounter = 0;

                    for (Tuple2<Integer, Record> t : records) {
                        Record r = t.getField1();
                        OffsetDateTime ts = OffsetDateTime.parse(r.getString(0));

                        if (!ts.equals(currentTs)) {
                            currentTs = ts;
                            snapshotIdx++;
                            gridCellCounter = 0;
                        }

                        int y = gridCellCounter / GRID_WIDTH;
                        int x = gridCellCounter % GRID_WIDTH;

                        reflectivityArray[snapshotIdx][y][x] = (float) r.getDouble(2);
                        precipitationArray[snapshotIdx][y][x] = (float) r.getDouble(3);

                        gridCellCounter++;
                    }

                    List<float[][][][]> X = new ArrayList<>();
                    List<float[][][][]> Y = new ArrayList<>();

                    for (int i = 0; i <= numSnapshots - (windowSizeIn + windowSizeOut); i++) {
                        float[][][][] XWindow = new float[windowSizeIn][2][GRID_HEIGHT][GRID_WIDTH];
                        float[][][][] YWindow = new float[windowSizeOut][2][GRID_HEIGHT][GRID_WIDTH];

                        for (int ch = 0; ch < 2; ch++) {
                            float[][][] sourceArray = (ch == 0) ? reflectivityArray : precipitationArray;

                            for (int j = 0; j < windowSizeOut; j++) {
                                XWindow[j][ch] = sourceArray[i + j];
                            }

                            for (int j = 0; j < windowSizeOut; j++) {
                                YWindow[j][ch] = sourceArray[i + windowSizeIn + j];
                            }
                        }
                        X.add(XWindow);
                        Y.add(YWindow);
                    }
                    return new Tuple2<>(X, Y);
                });
    }

    public static DLModel buildModel(
            int batchSize,
            int numChannelsIn,
            int numChannelsOut,
            int numKernels,
            int inputFrames,
            int outputFrames,
            int[] kernelSize,
            int numLayers,
            int[] stride
    ) {
        DLModel.Builder builder = new DLModel.Builder();

        builder
                .layer(new Input(new int[]{batchSize, inputFrames, numChannelsIn, GRID_HEIGHT, GRID_WIDTH}, Input.Type.FEATURES))
                .layer(new ConvLSTM2D(numChannelsIn, numKernels, kernelSize, stride, true, "output", "convlstm1"));

        for (int i = 2; i <= numLayers; i++) {
            builder.layer(new ConvLSTM2D(numKernels, numKernels, kernelSize, stride, true, "output", "convlstm" + i));
        }

        builder
                .layer(new Slice(new int[][]{{0, -1}, {inputFrames - outputFrames, -1}, {0, -1}, {0, -1}, {0, -1}})) // Input only the last outputFrames from ConvLSTM
                .layer(new Reshape(new int[]{batchSize, -1, GRID_HEIGHT, GRID_WIDTH}))
                .layer(new Conv2D(numKernels * outputFrames, outputFrames * numChannelsOut, kernelSize, stride, "SAME", true));

        return builder.build();
    }

    public static void main(String[] args) {

        Configurator.setLevel(WeatherForecastPipeline.class.getName(), org.apache.logging.log4j.Level.INFO);

        WayangContext wayangContext = new WayangContext()
                .with(Java.basicPlugin())
                .with(Tensorflow.plugin());

        planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WeatherForecast")
                .withUdfJarOf(WeatherForecastPipeline.class);

        logger.info("Reading data...");
        List<DataQuantaBuilder<?, Record>> allData = loadData();
        DataQuantaBuilder<?, Record> radarData = allData.get(0);
        DataQuantaBuilder<?, Record> rainGaugeData = allData.get(1);
        DataQuantaBuilder<?, Record> gridData = allData.get(2);

        logger.info("Adding coord column to data...");  // (0)
        radarData = radarData.map(
                row -> {
                    row.addField(new Tuple2<>(row.getDouble(1), row.getDouble(2)));
                    return row;
                }
        );
        rainGaugeData = rainGaugeData.map(
                row -> {
                    row.addField(new Tuple2<>(row.getDouble(1), row.getDouble(2)));
                    return row;
                }
        );
        gridData = gridData.map(
                row -> {
                    row.addField(new Tuple2<>(row.getDouble(0), row.getDouble(1)));
                    return row;
                }
        );

        logger.info("Finding nearest grid points...");  // (1)
        radarData = findNearestGridPoint(radarData, gridData);

        logger.info("Aggregating radar data...");  // (2)
        radarData = aggregateRadarData(radarData);

        logger.info("Applying IDW interpolation...");  // (3) (4)
        rainGaugeData = applyIdwInterpolation(rainGaugeData, gridData);

        logger.info("Joining radar and rain gauge data...");  // (5)
        DataQuantaBuilder<?, Record> data = joinRadarAndRainGauge(radarData, rainGaugeData);

        logger.info("Sorting data...");  // (6)
        data = sortData(data);

        logger.info("Splitting data into train and test sets...");  // (7)
        List<DataQuantaBuilder<?, Record>> trainAndTestData = trainTestSplit(data, dateToSplit);
        DataQuantaBuilder<?, Record> dataTrain = trainAndTestData.get(0);
        DataQuantaBuilder<?, Record> dataTest = trainAndTestData.get(1);

        logger.info("Normalizing data...");  // (8)
        List<DataQuantaBuilder<?, Record>> trainAndTestDataNormalized = normalizeData(dataTrain, dataTest);
        DataQuantaBuilder<?, Record> dataTrainNormalized = trainAndTestDataNormalized.get(0);
        DataQuantaBuilder<?, Record> dataTestNormalized = trainAndTestDataNormalized.get(1);

        dataTrainNormalized = dataTrainNormalized.sort(record -> record.getString(0));
        dataTestNormalized = dataTestNormalized.sort(record -> record.getString(0));

        logger.info("Building training and testing time series...");  // (9)
        DataQuantaBuilder<?, Tuple2<List<float[][][][]>, List<float[][][][]>>> trainingData = windowData(
                dataTrainNormalized,
                windowSizeIn,
                windowSizeOut
        );
        DataQuantaBuilder<?, float[][][][]> XTrain = trainingData.map(Tuple2::getField0).flatMap(x -> x);
        DataQuantaBuilder<?, float[][][][]> YTrain = trainingData.map(Tuple2::getField1).flatMap(x -> x);

        DataQuantaBuilder<?, Tuple2<List<float[][][][]>, List<float[][][][]>>> testingData = windowData(
                dataTestNormalized,
                windowSizeIn,
                windowSizeOut
        );
        DataQuantaBuilder<?, float[][][][]> XTest = testingData.map(Tuple2::getField0).flatMap(x -> x);
        DataQuantaBuilder<?, float[][][][]> YTest = testingData.map(Tuple2::getField1).flatMap(x -> x);

        logger.info("Building model");  // (9)
        int batchSize = 1;
        int numChannelsIn = 2;
        int numChannelsOut = 2;
        int numKernels = 64;
        int numInputFrames = windowSizeIn;
        int numOutputFrames = windowSizeOut;
        int[] kernelSize = new int[]{3, 3};
        int numLayers = 3;
        int[] stride = new int[]{1};

        DLModel model = buildModel(batchSize, numChannelsIn, numChannelsOut, numKernels, numInputFrames, numOutputFrames, kernelSize, numLayers, stride);

        /* training options */
        // 1. loss function
        Input labels = new Input(new int[]{batchSize, numOutputFrames, numChannelsOut, GRID_HEIGHT, GRID_WIDTH}, Input.Type.LABEL);

        Op criterion = new MSELoss();
        criterion.with(
                new Reshape(new int[]{-1}).with(model.getOut()),
                new Reshape(new int[]{-1}).with(labels)
        );

        // 2. optimizer
        float learningRate = 0.0001F;
        Optimizer optimizer = new Adam(learningRate);

        int epoch = 10;
        DLTrainingOperator.Option option = new DLTrainingOperator.Option(criterion, optimizer, batchSize, epoch);

        logger.info("Training model...");  // (10)
        DLTrainingDataQuantaBuilder<float[][][][], float[][][][]> trainingOperator = XTrain.dlTraining(YTrain, model, option);

        System.out.println(trainingOperator.collect());

    }
}
