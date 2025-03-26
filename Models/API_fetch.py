

import requests

url = "https://www.wikidata.org/w/api.php"

while True:
    query = input("Enter name (or 'exit' to quit): ")
    if query.lower() == "exit":
        break
    else:
        params = {
            "action": "wbsearchentities",
            "language": "en",
            "format": "json",
            "search": query
        }
        try:
            response = requests.get(url, params=params)
            results = response.json()["search"]
            if results:
                print("Label:", results[0]["label"])
                print("ID:", results[0]["id"])
                print("Description:", results[0].get("description", "No description available"))
            else:
                print("No results found")
        except requests.exceptions.RequestException as e:
            print("Request error:", e)
        except Exception as e:
            print("Something went wrong:", e)

