"""
(1a) Create a DataFrame

We'll start by generating a base DataFrame by using a Python list of tuples and the sqlContext.createDataFrame method. Then we'll print out the type and schema of the DataFrame. The Python API has several examples for using the createDataFrame method.
"""
wordsDF = sqlContext.createDataFrame([('cat',), ('elephant',), ('rat',), ('rat',), ('cat', )], ['word'])
wordsDF.show()
print type(wordsDF)
wordsDF.printSchema()

"""
(1b) Using DataFrame functions to add an 's'
Let's create a new DataFrame from wordsDF by performing an operation that adds an 's' to each word. To do this, we'll call the select DataFrame function and pass in a column that has the recipe for adding an 's' to our existing column. To generate this Column object you should use the concat function found in the pyspark.sql.functions module. Note that concat takes in two or more string columns and returns a single string column. In order to pass in a constant or literal value like 's', you'll need to wrap that value with the lit column function.
Please replace <FILL IN> with your solution. After you have created pluralDF you can run the next cell which contains two tests. If you implementation is correct it will print 1 test passed for each test.
This is the general form that exercises will take. Exercises will include an explanation of what is expected, followed by code cells where one cell will have one or more <FILL IN> sections. The cell that needs to be modified will have # TODO: Replace <FILL IN> with appropriate code on its first line. Once the <FILL IN> sections are updated and the code is run, the test cell can then be run to verify the correctness of your solution. The last code cell before the next markdown section will contain the tests.
Note: Make sure that the resulting DataFrame has one column which is named 'word'.
"""
# TODO: Replace <FILL IN> with appropriate code
from pyspark.sql.functions import lit, concat
#help(concat)
pluralDF = wordsDF.select(concat('word',lit('s')).alias('word'))
pluralDF.show()


"""
(1c) Length of each word
Now use the SQL length function to find the number of characters in each word. The length function is found in the pyspark.sql.functions module.
"""
# TODO: Replace <FILL IN> with appropriate code
from pyspark.sql.functions import length
pluralLengthsDF = pluralDF.select(length('word'))
pluralLengthsDF.show()

"""
(2a) Using groupBy and count
Using DataFrames, we can preform aggregations by grouping the data using the groupBy function on the DataFrame. Using groupBy returns a GroupedData object and we can use the functions available for GroupedData to aggregate the groups. For example, we can call avg or count on a GroupedData object to obtain the average of the values in the groups or the number of occurrences in the groups, respectively.
To find the counts of words, group by the words and then use the count function to find the number of times that words occur.
"""
# TODO: Replace <FILL IN> with appropriate code
wordCountsDF = (wordsDF
                .groupBy('word')
                .count())
wordCountsDF.show()


"""
(3a) Unique words
Calculate the number of unique words in wordsDF. You can use other DataFrames that you have already created to make this easier
"""
from spark_notebook_helpers import printDataFrames

#This function returns all the DataFrames in the notebook and their corresponding column names.
printDataFrames(True)

# TODO: Replace <FILL IN> with appropriate code
uniqueWordsCount = wordCountsDF.filter("count = 1").count()
print uniqueWordsCount


"""
(3b) Means of groups using DataFrames
Find the mean number of occurrences of words in wordCountsDF.
You should use the mean GroupedData method to accomplish this. Note that when you use groupBy you don't need to pass in any columns. A call without columns just prepares the DataFrame so that aggregation functions like mean can be applied.
"""
# TODO: Replace <FILL IN> with appropriate code
averageCount = (wordCountsDF
                .groupBy()
                .mean()
                .first()[0])

print averageCount


"""
(4a) The wordCount function
First, define a function for word counting. You should reuse the techniques that have been covered in earlier parts of this lab. This function should take in a DataFrame that is a list of words like wordsDF and return a DataFrame that has all of the words and their associated counts.
"""
# TODO: Replace <FILL IN> with appropriate code
def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('word').count()

wordCount(wordsDF).show()



"""
(4b) Capitalization and punctuation
Real world files are more complicated than the data we have been using in this lab. Some of the issues we have to address are:
Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
All punctuation should be removed.
Any leading or trailing spaces on a line should be removed.
Define the function removePunctuation that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces. Use the Python regexp_replace module to remove any text that is not a letter, number, or space. If you are unfamiliar with regular expressions, you may want to review this tutorial from Google. Also, this website is a great resource for debugging your regular expression.
You should also use the trim and lower functions found in pyspark.sql.functions.
Note that you shouldn't use any RDD operations or need to create custom user defined functions (udfs) to accomplish this task

"""
# TODO: Replace <FILL IN> with appropriate code
from pyspark.sql.functions import regexp_replace, trim, col, lower

def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    column = lower(column)
    column = regexp_replace(column, '[^a-zA-Z0-9\s]+', '')
    column = trim(column) 
    return column.alias('sentence')

sentenceDF = sqlContext.createDataFrame([('Hi, you!',),
                                         (' No under_score!',),
                                         (' *      Remove punctuation then spaces  * ',)], ['sentence'])
sentenceDF.show(truncate=False)
(sentenceDF
 .select(removePunctuation(col('sentence')))
 .show(truncate=False))


"""
(4c) Load a text file
For the next part of this lab, we will use the Complete Works of William Shakespeare from Project Gutenberg. To convert a text file into a DataFrame, we use the sqlContext.read.text() method. We also apply the recently defined removePunctuation() function using a select() transformation to strip out the punctuation and change all text to lower case. Since the file is large we use show(15), so that we only print 15 lines.
"""

fileName = "dbfs:/databricks-datasets/cs100/lab1/data-001/shakespeare.txt"

shakespeareDF = sqlContext.read.text(fileName).select(removePunctuation(col('value')))
shakespeareDF.show(15, truncate=False)


"""
(4d) Words from lines
Before we can use the wordcount() function, we have to address two issues with the format of the DataFrame:
The first issue is that that we need to split each line by its spaces.
The second issue is we need to filter out empty lines or words.
Apply a transformation that will split each 'sentence' in the DataFrame by its spaces, and then transform from a DataFrame that contains lists of words into a DataFrame with each word in its own row. To accomplish these two tasks you can use the split and explode functions found in pyspark.sql.functions.
Once you have a DataFrame with one word per row you can apply the DataFrame operation where to remove the rows that contain ''.
Note that shakeWordsDF should be a DataFrame with one column named word.
"""

# TODO: Replace <FILL IN> with appropriate code
from pyspark.sql.functions import split, explode
shakeWordsDF = (shakespeareDF
                .select(
                 split('sentence', ' ') 
                .alias('sentence')
                ))
shakeWordsDF = (shakeWordsDF
                .select(
                 explode('sentence')
                .alias('word')
                ).filter("word != ''"))
shakeWordsDF.show()
shakeWordsDFCount = shakeWordsDF.count()
print shakeWordsDFCount


"""
(4e) Count the words
We now have a DataFrame that is only words. Next, let's apply the wordCount() function to produce a list of word counts. We can view the first 20 words by using the show() action; however, we'd like to see the words in descending order of count, so we'll need to apply the orderBy DataFrame method to first sort the DataFrame that is returned from wordCount().
You'll notice that many of the words are common English words. These are called stopwords. In a later lab, we will see how to eliminate them from the results.
"""

# TODO: Replace <FILL IN> with appropriate code
from pyspark.sql.functions import desc
totalWordscount = wordCount(shakeWordsDF)
topWordsAndCountsDF = totalWordscount.orderBy(totalWordscount['count'].desc())
topWordsAndCountsDF.show()