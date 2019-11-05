package SassWI.transformations

import SassWI.transformations.Etl.{EtlToLowerCase, codeToInterest, colsToLabels, explodeInterests, interestsAsList, listToVector}
import org.apache.spark.sql

object TransformDataset {
  def transform(originalDf: sql.DataFrame, interests: sql.DataFrame): sql.DataFrame = {
    val df2 = originalDf.transform(EtlToLowerCase)
      .transform(interestsAsList)
      .transform(codeToInterest(interests))
    df2.show()

    colsToLabels(df2, df2.columns)
      .transform(explodeInterests(interests))
      .transform(listToVector)
  }
}
