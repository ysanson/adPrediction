#  Description
This program create a model to predict if the user clicks or not from a dataset of advertising data. The model is already saved you have just to give your data and the trained model will be apply on it.
In input the program takes a JSON file. 
In output it gives a CSV with the first column which contains the predicted values.

# Instructions to launch the program
To launch the program follow the instructions: 
* Clone the current git repository.
* In the terminal, move to the root folder of the cloned project.
* Do the command `sbt assembly` to create the JAR file. 
* Execute the command `mv target/scala2.12/adPrediction-assembly-1.0.jar`
* Launch with `java -jar adPrediction.jar [param]` param is run or predict. run is for re train the model and predict is for predict the label of a data file.