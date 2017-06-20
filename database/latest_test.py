import database_proxy
import json 
result = database_proxy.get_latest_tweet()
print(result.fetchall())