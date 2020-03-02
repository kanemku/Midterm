import string
from pyspark import SparkConf, SparkContext
from typing import List

# I couldn't figure out how to iterate through the folder to grab the different encrypted files to run them through the program,
# so going through each file has to be done manually. This includes changing the key in the decrypt function. I had to manually 
# go through each file and do trial and error until I found the correct key to shift the cipher by.
# The code has to be manually changed to get the correct values of each file, so for Encrypted-1.txt, you have to manually change 
# the key in decrypt to 13. It should look like decrypt(13, decrypted), and for Enc2 and 3, it should look like decrypt(19, decrypted) 
# and decrypt(5, decrypted) respectively. Also, wherever there's a filepath, that needs to be changed manually in the same fashion that 
# decrypt needs to be changed. I will go through this code and try to figure out a more efficient way so that the user doesn't need to 
# change the code manually. 

conf = SparkConf().setMaster("local").setAppName("Caesar Cypher")
sc = SparkContext(conf = conf)

# Use "with open()..." to read encrypted text as a string to make it easier to decrypt.
with open('/Users/kane/School/BigData/Midterm/Encrypted-1.txt','r') as file:\
	decrypted = file.read().replace('\n', '')

# Create decrypting function
def decrypt(key, txtFile):
    txtFile = decrypted.upper()
    alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    result = ""
    # Iterate through every character in string
    for char in txtFile: 
    	# If the character resides in the value of variable "alpha," find the 
		# position of the character and subtract the key value from that value.
        if char in alpha:
            char_index = (alpha.find(char) - key) % len(alpha)
            # The result is the resultant of the above formula which corresponds with a 
			# different letter in the alphabet. It replaces the old char with the new char.
            result = result + alpha[char_index]
        else:
            result = result + char
    return result

    # Encrypted 1 is from The New Hacker's Dictionary. The shift was 13.
    # Encrypted 2 is from An Englishman's Travels in America: His Observations of Life and Manners in the Free and Slave States. The shift was 19.
    # Encrypted 3 is from The Great Gatsby. The shift was 5.


# Once the file is decrypted, convert it back into an RDD from a string, and run our word and char counts. 
result = decrypt(13, decrypted)
answer = sc.parallelize([result]).collect()
RDD = sc.parallelize(answer)
RDD.saveAsTextFile("/Users/kane/School/BigData/Midterm/ENC1/")

enc1 = sc.textFile("/Users/kane/School/BigData/Midterm/ENC1/part-00000")

lines = enc1.map(lambda line: str(line))
lines.persist()

words = lines.flatMap(lambda string: string.split())
chars = lines.flatMap(lambda string: list(string))

lines.unpersist()

total_words = words.count()
total_chars = chars.count()

wordsCount = words.map(lambda word: (word.lower(), 1)) \
	.reduceByKey(lambda numOfWord1, numOfWord2: numOfWord1 + numOfWord2) \
		.map(lambda word: (word[0],word[1], word[1]/float(total_words)*100))


wordsRep = words.map(lambda word: (word.lower(), word)) \
	.reduceByKey(lambda word1, word2: word1 + " " + word2 if word2 not in word1 else word1)

wordsFinal = wordsCount.join(wordsRep)

charsCount = chars.map(lambda char: (char.lower(), 1)) \
	.reduceByKey(lambda numOfchar1, numOfchar2: numOfchar1 + numOfchar2) \
		.map(lambda char: (char[0],char[1], char[1]/float(total_chars)*100))