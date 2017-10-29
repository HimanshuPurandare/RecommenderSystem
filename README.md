# Recommendation system using Item-based collaborative filtering
## Dataset used: MovieLens

## Task 1:
## Model-based CF Algorithm:
Predict absent ratings for testing dataset.<br />
A Model-based CF recommendation system by using Spark MLlib.<br />
Model Used: ALS algorithm<br />
Accuracy obtained: RMSE = 1.0006<br />
More Information: (https://spark.apache.org/docs/2.2.0/mllib-collaborative-filtering.html "Collaborative Filtering - RDD-based API")

## Task 2:
## Item-based collaborative filtering using Pearson correlation:
Predict absent ratings for testing dataset.<br />
Accuracy obtained: RMSE = 0.9742

## Questions:
* How have you handled the missing users, outlier rating when predicted value exceeds [0,5] range?
The normal algorithms do not take into account the missing users/movies in the testing data set. For that I did the “Simple Mean Imputation”.<br />
    1) That means that for the movies which are not present in the training data, I just took the mean of all the ratings given by the respective user as the rating of the item.<br />
    2) If the user is not present then it gives the average rating of the movie to it.<br />
    3) If both the user and the movie are not present in the training data set, then I have set the prediction as 2.5<br />
<br />
To handle the outlier ratings, if the rating goes above 5 or below 0, then I am taking the average of the movie and assigning it as the
rating.<br />

* How to run your program for both tasks?
**Task 1:**
1) spark-submit --class "Himanshu_Purandare_Task1" --master local[*] Himanshu_Purandare_task1.jar "<Path_to_Ratings.csv>"
"*<path_to_testing_small.csv>*"
**Task 2:**
1) spark-submit --class "Himanshu_Purandare_Task2" --master local[*] Himanshu_Purandare_task2.jar "*<Path_to_Ratings.csv>*"
"*<Path_to_Testing_small.csv>*"

* Any improvement in your recommendation system?
I have taken neighbourhood as 20 to get better results.<br />
Also, I have used Case Amplification for getting better results. By doing Case amplification, we can reduce noise and thus get better results.<br />
Case Amplification favours high weights while penalises smaller weights. Thus, this changes the neighbourhood of the predictions.
