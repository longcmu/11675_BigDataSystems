# KNN-Hadoop

Source code:
distanceLabel.java
KNNDriver.java
KNNMapper.java
KNNReducer.java

Prediction output:
part-r-00000

This program can be called like this:
"hadoop jar yourprogram.jar input_path output_path k (train_path)""

E.g.
"hadoop jar KNN.jar /user/hue/KNNDriver/iris_test_data.csv /user/hue/KNNOutput/out 5 /user/hue/KNNDriver/iris_train.csv"

