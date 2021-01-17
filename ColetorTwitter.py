import twint
import json
import splunklib.client as client

def printResults(resultados):
	for result in resultados:
		print(result)
		print(dir(result))
		print("Time: " + str(result.datetime))
		print("Date: " + str(result.datestamp))
		print("Usuario (nome): " + str(result.username))
		print("Usuario (real): " + str(result.user_id_str))
		print("Geo: " + str(result.geo))
		print("Hashtags: " + str(result.hashtags))
		print("Likes: " + str(result.likes_count))
		print("RTs: " + str(result.retweets_count)) 
		print("Mentions: " + str(result.mentions))
		print("Text: " + str(result.tweet))
		
		print("\n\n")
		
def createTweetJson(tweet):
	tweetDict = vars(tweet)
	tweetJson = json.dumps(tweetDict)
	return tweetJson
	
def openSplunkConnection():	
	service = client.connect(host='localhost',port=8089,username='admin',password='admin123')
	indexConection = service.indexes["main"] 	
	return indexConection
	
def sendJsonToSplunk(jsonData, indexConection): 
	indexConection.submit(jsonData)

#################### MAIN ####################
	
resultados = []

# Configure
c = twint.Config()
c.Store_object = True
c.Store_object_tweets_list = resultados
c.Hide_output = True
c.Limit = 10

#Search params
c.Username = "santander"

#Open splunk connection
indexConection = openSplunkConnection()

# Run
twint.run.Search(c)
for result in resultados:
	resultJson = createTweetJson(result)
	print(resultJson)
	print("\n")
	sendJsonToSplunk(resultJson, indexConection)

print(str(len(resultados)) + " tweets coletados")

