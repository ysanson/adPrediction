#  Description

This program create a model to predict if the user clicks or not from a dataset of advertising data. The model is already saved you have just to give your data and the trained model will be apply on it.
In input the program takes a JSON file. 
In output it gives a CSV with the first column which contains the predicted values.

# Instructions to launch the program

To launch the program follow the instructions: 
* Clone the current git repository.
* In the terminal, move to the root folder of the cloned project.
* Do the command `sbt assembly` to create the JAR file. 
* Execute the command `mv target/scala2.12/adprediction-assembly-1.0.jar adprediction.jar`
* Launch with `java -jar adPrediction.jar [param]` param is run or predict. run is for re train the model and predict is for predict the label of a data file.

## Train a new model

To train a new model, first make sure a file called `data-students.json` exists in the project's directory, and that it contains a _label_ column.

Please make sure that if a folder named `models` exists, you delete it beforehand.

Then, train a new model by calling `java -jar adprediction.jar train`. It should take a few minutes, and the results are in `models/LogisticRegression`. The metrics for the trained model are shown during the process.

## Predict

The prediction _requires_ a model to be created beforehand; the folder `models/LogisticRegression` must exist and it must not be empty.
If you have an `output` folder, please delete it before running the prediction.
To predict the outcome of input values, run `java -jar adprediction.jar predict [filename]`, where _filename_ is the path to a JSON file.
Please note that if your data contains a _label_ attribute, it will be replaced during the process by predicted values.

The results are stored in a folder called `output`. Inside, you will find some files, namely one CSV containing the results. The CSV's name changes because of Spark implementation of workers, but it should always follow the scheme `part-0000-xxxx.csv`.
 
The predicted label is stored in the first column, called label, and the value varies from true to false.
