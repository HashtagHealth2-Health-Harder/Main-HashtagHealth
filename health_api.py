from flask import Flask, jsonify, json
import database_proxy

app = Flask(__name__)

@app.route('/latest', methods=['GET'])
def get_latest():
	tweet = database_proxy.get_latest_tweet()
	return str(tweet)

if __name__ == '__main__':
	app.debug = True
	app.run(port = 8000)