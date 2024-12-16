import requests
import pandas as pd
import time

class RobloxGameScraper:
    def __init__(self):
        self.api_url = "https://apis.roblox.com/explore-api/v1/get-sorts"
        self.game_details_url = "https://games.roblox.com/v1/games"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }

    def fetch_game_ids_from_api(self, session_id, max_games=60000):
        """
        Fetch a list of game IDs using Roblox's API with pagination.
        Stop when the total number of games reaches max_games.
        """
        all_game_data = []
        sorts_page_token = None  # Start without a token

        while len(all_game_data) < max_games:
            params = {
                "sessionId": session_id,
                "device": "computer",
                "country": "all",
                "cpuCores": 24,
                "maxResolution": "1707x1067",
                "maxMemory": 8192,
                "networkType": "4g",
            }
            if sorts_page_token:
                params["sortsPageToken"] = sorts_page_token

            response = requests.get(self.api_url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Failed to fetch data from API: {response.status_code}")
                break

            try:
                data = response.json()
                sorts = data.get("sorts", [])
                for sort in sorts:
                    games = sort.get("games", [])
                    for game in games:
                        game_data = {
                            "UniverseId": game.get("universeId"),
                            "Name": game.get("name", "N/A"),
                            "PlayerCount": game.get("playerCount", 0),
                            "Upvotes": game.get("totalUpVotes", 0),
                            "Downvotes": game.get("totalDownVotes", 0)
                        }
                        if game_data["UniverseId"]:
                            all_game_data.append(game_data)

                # Update sortsPageToken for next page
                sorts_page_token = data.get("nextSortsPageToken")
                if not sorts_page_token:  # No more pages
                    print("No more pages available.")
                    break
            except Exception as e:
                print(f"Error parsing API response: {e}")
                break

            print(f"Total games collected: {len(all_game_data)}")
            time.sleep(1)  # Add delay between requests to avoid hitting rate limits

        return all_game_data[:max_games]  # Limit to max_games

    def fetch_game_details(self, universe_ids):
        """
        Fetch details for multiple games using their Universe IDs in a single request.
        """
        params = {"universeIds": ",".join(map(str, universe_ids))}
        response = requests.get(self.game_details_url, headers=self.headers, params=params)

        if response.status_code == 429:
            print("Rate limit hit. Retrying in 5 seconds...")
            time.sleep(5)
            return self.fetch_game_details(universe_ids)  # Retry after delay

        if response.status_code != 200:
            print(f"Failed to fetch data for universe IDs {universe_ids}: {response.status_code}")
            return []

        try:
            data = response.json()
            if "data" in data:
                return [
                    {
                        "UniverseId": game.get("id"),
                        "Name": game.get("name", "N/A"),
                        "Genre": game.get("genre", "N/A"),
                        "Created Date": game.get("created", "N/A"),
                        "Updated Date": game.get("updated", "N/A"),
                        "Max Players": game.get("maxPlayers", "N/A"),
                        "Playability Status": game.get("playabilityStatus", "N/A"),
                        "Is Experimental": game.get("isExperimental", False),
                        "Price": game.get("price", 0),
                        "Visits": game.get("visits", 0),
                        "Developer": game.get("creator", {}).get("name", "N/A"),
                        "Thumbnail URL": game.get("thumbnailUrl", "N/A")
                    }
                    for game in data["data"]
                ]
        except Exception as e:
            print(f"Error parsing API response: {e}")
            return []

    def scrape_games(self, game_data):
        """
        Scrape additional details for games using their Universe IDs in batches.
        """
        games_data = []
        batch_size = 50  # Fetch 50 games at a time
        universe_ids = [game["UniverseId"] for game in game_data]

        for i in range(0, len(universe_ids), batch_size):
            batch_ids = universe_ids[i:i + batch_size]
            print(f"Fetching data for Universe IDs: {batch_ids}")
            batch_data = self.fetch_game_details(batch_ids)
            # Merge basic game data with detailed data
            for detail in batch_data:
                for basic in game_data:
                    if basic["UniverseId"] == detail["UniverseId"]:
                        detail.update({
                            "PlayerCount": basic.get("PlayerCount", 0),
                            "Upvotes": basic.get("Upvotes", 0),
                            "Downvotes": basic.get("Downvotes", 0)
                        })
                        games_data.append(detail)
                        break
            time.sleep(1)  # Delay to avoid hitting rate limits

        return games_data

    def save_to_csv(self, games_data, file_name="roblox_games.csv"):
        """
        Save the scraped data to a CSV file.
        """
        df = pd.DataFrame(games_data)
        df.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}")

if __name__ == "__main__":
    scraper = RobloxGameScraper()

    # Session ID
    session_id = "4c77f6d0-a42c-4b37-9ca2-42ce3ca0800f"

    # Step 1: Fetch game IDs and basic data using API
    basic_game_data = scraper.fetch_game_ids_from_api(session_id, max_games=60000)

    # Step 2: Scrape detailed game data
    detailed_games_data = scraper.scrape_games(basic_game_data)

    # Step 3: Save data to CSV
    scraper.save_to_csv(detailed_games_data)
