package org.apache.wayang.java.mapping;

import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaFlatMapOperator;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link FlatMapOperator} to {@link JavaFlatMapOperator}.
 */
@SuppressWarnings("unchecked")
public class FlatMapMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        JavaPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "flatMap", new FlatMapOperator<>(null, DataSetType.none(), DataSetType.none()), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<FlatMapOperator>(
                (matchedOperator, epoch) -> new JavaFlatMapOperator<>(matchedOperator).at(epoch)
        );
    }
}