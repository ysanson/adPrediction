package SassWI.transformations

import SassWI.transformations.Etl.{EtlToLowerCase, codeToInterest, colsToLabels, explodeInterests, interestsAsList, listToVector}
import org.apache.spark.sql

object TransformDataset {
  def transform(originalDf: sql.DataFrame, interests: sql.DataFrame): sql.DataFrame = {
    originalDf.transform(EtlToLowerCase)
      .transform(interestsAsList)
      .transform(codeToInterest(interests))
      .transform(colsToLabels)
      .transform(explodeInterests(interests))
      .transform(listToVector)
  }
}
