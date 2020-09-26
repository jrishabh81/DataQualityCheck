package com.lwwale;

import com.lwwale.exception.ApplicationException;
import com.lwwale.models.DqCheckModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class DataQualityCheck implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataQualityCheck.class);

    public Dataset<Row> performDqCheck(Dataset<Row> inputDataSet, List<DqCheckModel> dqCheckModels) {
        if (inputDataSet == null) {
            LOGGER.error("Input dataset found to be null. Halting further operations");
            throw new ApplicationException("Input dataset null");
        }
        LOGGER.info("Performing DQ check");
        if (LOGGER.isDebugEnabled()) LOGGER.debug(inputDataSet.schema().toString());
        dqCheckModels.sort(Comparator.comparingInt(DqCheckModel::getSequence));
        Dataset<Row> finalDataset = inputDataSet;
        for (DqCheckModel dqCheckModel : dqCheckModels) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Evaluating DQ CheckStep : {}", dqCheckModel);
            if (dqCheckModel.getOutputColumnName() != null) {
                finalDataset = dqCheckModel.getDqCheckType().evaluate(finalDataset, dqCheckModel.getColumnName(), dqCheckModel.getOutputColumnName(), dqCheckModel.getParameters());
            } else {
                finalDataset = dqCheckModel.getDqCheckType().evaluate(finalDataset, dqCheckModel.getColumnName(), dqCheckModel.getParameters());
            }
        }
        LOGGER.debug("Completed DQ Evaluation");
        if (LOGGER.isDebugEnabled()) LOGGER.debug(finalDataset.schema().toString());
        return finalDataset;
    }
}
