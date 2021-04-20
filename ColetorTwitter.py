import twint
import json
import splunklib.client as client
import sqlite3 as sl

tweetsParaBuscar = 4000
splunkUsername = "admin"
splunkPassword = "admin123"

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
	service = client.connect(host='localhost',port=8089,username=splunkUsername,password=splunkPassword)
	indexConection = service.indexes["main"] 	
	return indexConection
	
def sendJsonToSplunk(jsonData, indexConection): 
	indexConection.submit(jsonData, sourcetype="twitter")
	
def verificarTweetJaVisto(tweetId):
	with dbConnection:
		data = dbConnection.execute("SELECT COUNT(*) FROM TWEETS WHERE tweetid = " + str(tweetId))
		for row in data:
			if (row[0] == 0):
				return False
			else:
				return True
				
def adicionarATweetsJavistos(tweet):
	try:
		with dbConnection:
			dbConnection.execute(" INSERT INTO TWEETS VALUES (" + str(tweet.id) + ")")
	except:
		pass

def removerDuplicados(resultados):
	resultadosNovos = []
	for result in resultados:
		tweetid = result.id
		if(not verificarTweetJaVisto(tweetid)):
			resultadosNovos.append(result)
	return resultadosNovos
	
def procurarTweets(termo):
	resultados = []

	# Configure
	c = twint.Config()
	c.Store_object = True
	c.Store_object_tweets_list = resultados
	c.Hide_output = True
	c.Limit = tweetsParaBuscar
	c.Store_json = True
	c.Filter_retweets = False
	c.Profile_fulle = True

	#Search params
	c.All = termo

	# Run
	twint.run.Search(c)
	print(termo + " - " + str(len(resultados)) + " tweets coletados")
	
	return resultados

#################### MAIN ####################
	
#Inicializar banco de dados que armazena tweets duplicados:
dbConnection = sl.connect('dados.db')
try:
	with dbConnection:
		dbConnection.execute("""
			CREATE TABLE TWEETS (
				tweetid INTEGER NOT NULL PRIMARY KEY
			); 
		""")
except:
	print("Banco de dados ja existe")
	
resultadosTotal = []
resultadosTotal = resultadosTotal + procurarTweets("AprendaComTC")
resultadosFinal = removerDuplicados(resultadosTotal)

if (len(resultadosFinal) > 0):

	#Open splunk connection
	indexConection = openSplunkConnection()
	for result in resultadosFinal:
		resultJson = createTweetJson(result)
		print(resultJson)
		print("\n")
		sendJsonToSplunk(resultJson, indexConection)
		adicionarATweetsJavistos(result)
	print(str(len(resultadosFinal)) + " tweets coletados")

else:
	print("Nenhum tweet novo encontrado")



