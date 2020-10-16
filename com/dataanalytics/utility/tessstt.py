import regex
print(regex.findall(r'(?r)\w+(?=\W+\w*e)', 'John likes to eat mushrooms'))


stringgg= r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\com\dataanalytics\utility\messageLogger.py"
*middle, word_list = stringgg.split("\\")

#print(regex.findall(r'(?r)\w+', stringgg ))
print(word_list)